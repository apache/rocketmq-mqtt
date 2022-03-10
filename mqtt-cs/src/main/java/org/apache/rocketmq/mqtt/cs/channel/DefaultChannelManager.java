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

import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
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
                hashedWheelTimer.newTimeout(timeout.task(), (long)Math.ceil(keepAliveTimeSeconds * 1.5 + 1),
                    TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    @Override
    public void closeConnect(Channel channel, ChannelCloseFrom from, String reason) {
        String clientId = ChannelInfo.getClientId(channel);
        String channelId = ChannelInfo.getId(channel);
        if (clientId == null) {
            channelMap.remove(channelId);
            sessionLoop.unloadSession(clientId, channelId);
            if (channel.isActive()) {
                channel.close();
            }
            return;
        }

        //session maybe null
        Session session = sessionLoop.unloadSession(clientId, channelId);
        retryDriver.unloadSession(session);
        channelMap.remove(channelId);

        ChannelInfo.clear(channel);

        if (channel.isActive()) {
            channel.close();
        }
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
