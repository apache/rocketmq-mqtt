/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.cs.channel;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.commons.lang3.StringUtils;

import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.common.util.HostInfo;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.ds.upstream.processor.PublishProcessor;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class DefaultChannelManager implements ChannelManager {
    private static Logger logger = LoggerFactory.getLogger(DefaultChannelManager.class);
    private Map<String, Channel> channelMap = new ConcurrentHashMap<>(1024);
    private HashedWheelTimer hashedWheelTimer;
    private static int minBlankChannelSeconds = 10;
    private ScheduledThreadPoolExecutor scheduler;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private RetryDriver retryDriver;

    @Resource
    private MqttMsgId mqttMsgId;

    @Resource
    private MetaClient metaClient;

    @Resource
    private PublishProcessor publishProcessor;

    @Resource
    private InFlyCache inFlyCache;


    @PostConstruct
    public void init() {
        sessionLoop.setChannelManager(this);
        hashedWheelTimer = new HashedWheelTimer(1, TimeUnit.SECONDS);
        hashedWheelTimer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Channel channel : channelMap.values()) {
                closeConnect(channel, ChannelCloseFrom.SERVER, "ServerShutdown");
            }
        }));
    }

    @Override
    public void addChannel(Channel channel) {
        if (channelMap.size() > connectConf.getMaxConn()) {
            closeConnect(channel, ChannelCloseFrom.SERVER, "overflow");
            logger.error("channel is too many {}", channelMap.size());
            return;
        }
        ChannelInfo.touch(channel);
        channelMap.put(ChannelInfo.getId(channel), channel);
        hashedWheelTimer.newTimeout(timeout -> doPing(timeout, channel), minBlankChannelSeconds, TimeUnit.SECONDS);
    }

    private void doPing(Timeout timeout, Channel channel) {
        try {
            if (StringUtils.isBlank(ChannelInfo.getClientId(channel))) {
                //close
                closeConnect(channel, ChannelCloseFrom.SERVER, "No CONNECT");
                return;
            }
            long channelLifeCycle = ChannelInfo.getChannelLifeCycle(channel);
            if (System.currentTimeMillis() > channelLifeCycle) {
                closeConnect(channel, ChannelCloseFrom.SERVER, "Channel Auth Expire");
                return;
            }
            if (ChannelInfo.isExpired(channel)) {
                closeConnect(channel, ChannelCloseFrom.SERVER, "No Heart");
            } else {
                int keepAliveTimeSeconds = ChannelInfo.getKeepLive(channel);
                long lastTouchTime = ChannelInfo.getLastTouch(channel);
                long heartWindow = (long) Math.ceil(keepAliveTimeSeconds * 1.5 + 1) * 1000L;
                long delay = Math.min(heartWindow, heartWindow - (System.currentTimeMillis() - lastTouchTime));
                if (delay <= 0) {
                    delay = heartWindow;
                }
                hashedWheelTimer.newTimeout(timeout.task(), delay, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            logger.error("Exception when doPing: ", e);
        }
    }

    @Override
    public void closeConnect(Channel channel, ChannelCloseFrom from, String reason) {
        String clientId = ChannelInfo.getClientId(channel);
        String channelId = ChannelInfo.getId(channel);
        String ip = IpUtil.getLocalAddressCompatible();

        String willKey = ip + Constants.CTRL_1 + clientId;

        if (metaClient.bContainsKey(willKey)) {
            if (!reason.equals("disconnect")) {
                WillMessage willMessage = JSON.parseObject(new String(metaClient.bGet(willKey)), new TypeReference<WillMessage>() {
                });

                int mqttId = mqttMsgId.nextId(clientId);
                MqttPublishMessage mqttMessage = MessageUtil.toMqttMessage(willMessage.getWillTopic(), willMessage.getBody(), willMessage.getQos(), mqttId);
                final MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
                final boolean isQos2Message = isQos2Message(mqttMessage);
                if (isQos2Message && !inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, variableHeader.packetId())) {
                    inFlyCache.put(InFlyCache.CacheType.PUB, channelId, variableHeader.packetId());
                }
                publishProcessor.process(buildMqttMessageUpContext(channel), mqttMessage);
            }

            metaClient.delete(willKey).whenComplete((result, throwable) -> {
                if (!result || throwable != null) {
                    logger.error("fail to delete will message key", willKey);
                    return;
                }
                logger.info("delete will message key {} successfully", willKey);
            });
        }

        if (clientId == null) {
            channelMap.remove(channelId);
            sessionLoop.unloadSession(clientId, channelId);
        } else {
            //session maybe null
            Session session = sessionLoop.unloadSession(clientId, channelId);
            retryDriver.unloadSession(session);
            channelMap.remove(channelId);
            ChannelInfo.clear(channel);
        }

        if (channel.isActive()) {
            channel.close();
        }
        logger.info("Close Connect of channel {} from {} by reason of {}", channel, from, reason);
    }

    private MqttMessageUpContext buildMqttMessageUpContext(Channel channel) {
        MqttMessageUpContext context = new MqttMessageUpContext();
        context.setClientId(ChannelInfo.getClientId(channel));
        context.setChannelId(ChannelInfo.getId(channel));
        context.setNode(HostInfo.getInstall().getAddress());
        context.setNamespace(ChannelInfo.getNamespace(channel));
        return context;
    }

    private boolean isQos2Message(MqttPublishMessage mqttPublishMessage) {
        return MqttQoS.EXACTLY_ONCE.equals(mqttPublishMessage.fixedHeader().qosLevel());
    }


    @Override
    public void closeConnect(String channelId, String reason) {
        Channel channel = channelMap.get(channelId);
        if (channel == null) {
            return;
        }
        closeConnect(channel, ChannelCloseFrom.SERVER, reason);
    }

    @Override
    public Channel getChannelById(String channelId) {
        return channelMap.get(channelId);
    }

    @Override
    public int totalConn() {
        return channelMap.size();
    }

}
