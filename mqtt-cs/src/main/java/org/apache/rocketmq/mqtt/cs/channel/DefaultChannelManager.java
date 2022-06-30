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
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;

import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
    private PushAction pushAction;

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private MetaClient metaClient;

    @Resource
    private WildcardManager wildcardManager;

    @Resource
    private FirstTopicManager firstTopicManager;


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
                CompletableFuture<Boolean> future = MQHandleWillMessage(willMessage, clientId);
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

    /**
     * distribute will message through MQ
     *
     * @param willMessage
     * @param clientId
     * @return
     */
    private CompletableFuture<Boolean> MQHandleWillMessage(WillMessage willMessage, String clientId) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        int mqttId = mqttMsgId.nextId(clientId);
        MqttMessage mqttMessage = MessageUtil.toMqttMessage(willMessage.getWillTopic(), willMessage.getBody(), willMessage.getQos(), mqttId);
        String willTopic = willMessage.getWillTopic();
        String pubTopic = TopicUtils.normalizeTopic(willTopic);
        MqttTopic mqttTopic = TopicUtils.decode(pubTopic);
        firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic());
        Set<String> queueNames = wildcardManager.matchQueueSetByMsgTopic(pubTopic, null);
        Message message = MessageUtil.toMessage((MqttPublishMessage) mqttMessage);
        String msgId = MessageClientIDSetter.createUniqID();
        message.setMsgId(msgId);
        message.setBornTimestamp(System.currentTimeMillis());
//        Set<String> queueNames = new HashSet<>();
//        queueNames.add(willTopic);

        CompletableFuture<StoreResult> storeResultFuture = lmqQueueStore.putMessage(queueNames, message);
        storeResultFuture.whenComplete((storeResult, throwable) -> {
            logger.info("will message : {}, {}, {}", storeResult.getQueue().getBrokerName(), storeResult.getQueue().getQueueName(), storeResult.getMsgId());
            if (throwable != null) {
                logger.error("fail to send will message : {}, {}, {}", storeResult.getQueue().getBrokerName(), storeResult.getQueue().getQueueName(), storeResult.getMsgId());
            }
            result.complete(throwable == null);
        });

        return result;
    }

    /**
     * @param willMessage distribute will message to clients that subscribe the will topic
     */
    private boolean handleWillMessage(WillMessage willMessage) {
        // get all clients subscribing the will topic
        Set<Session> sessions = new HashSet<>();
        for (Channel channel : channelMap.values()) {
            Session session = sessionLoop.getSession(ChannelInfo.getId(channel));
            if (session == null) {
                continue;
            }
            Set<Subscription> tmp = session.subscriptionSnapshot();
            if (tmp != null && !tmp.isEmpty()) {
                for (Subscription subscription : tmp) {
                    if (TopicUtils.isMatch(willMessage.getWillTopic(), subscription.getTopicFilter())) {
                        sessions.add(session);
                        break;
                    }
                }
            }
        }

        // distribute will message
        int qos = willMessage.getQos();
        ChannelFuture flushFuture = null;
        boolean isFlushSuccess = true;
        Integer subMaxQos = -1;
        for (Session session : sessions) {
            int mqttId = mqttMsgId.nextId(session.getClientId());
            MqttMessage message = MessageUtil.toMqttMessage(willMessage.getWillTopic(), willMessage.getBody(), willMessage.getQos(), mqttId);
            Set<Subscription> tmp = session.subscriptionSnapshot();
            if (tmp != null && !tmp.isEmpty()) {
                for (Subscription subscription : tmp) {
                    if (TopicUtils.isMatch(willMessage.getWillTopic(), subscription.getTopicFilter())) {
                        subMaxQos = Math.max(subMaxQos, subscription.getQos());
                    }
                }
            }
            qos = Math.min(subMaxQos, qos);
            if (qos == 0) {
                if (!session.getChannel().isWritable()) {
                    logger.error("UnWritable:{}", session.getClientId());
                    continue;
                }
                try {
                    flushFuture = session.getChannel().writeAndFlush(message).sync();
                } catch (InterruptedException e) {
                    isFlushSuccess = false;
                    logger.error(flushFuture.toString(), "flush process is not correct");
                    throw new RuntimeException(e);
                }
                pushAction.rollNextByAck(session, mqttId);
            } else {
                retryDriver.mountPublish(mqttId, MessageUtil.toMessage((MqttPublishMessage) message), willMessage.getQos(), ChannelInfo.getId(session.getChannel()), null);
                try {
                    flushFuture = session.getChannel().writeAndFlush(message).sync();
                } catch (InterruptedException e) {
                    isFlushSuccess = false;
                    logger.error(flushFuture.toString(), "flush process is not correct");
                    throw new RuntimeException(e);
                }
            }
        }
        if (willMessage.isRetain()) {
            //complete this part after retainMessage is complete
        }

        return isFlushSuccess;
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
