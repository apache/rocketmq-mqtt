/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Component
public class MqttConnectHandler implements MqttPacketHandler<MqttConnectMessage> {
    private static Logger logger = LoggerFactory.getLogger(MqttConnectHandler.class);

    @Resource
    private ChannelManager channelManager;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private ConnectConf connectConf;

    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("check_connect_future"));

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttConnectMessage connectMessage, HookResult upstreamHookResult) {
        MqttConnectVariableHeader variableHeader = connectMessage.variableHeader();
        Channel channel = ctx.channel();
        ChannelInfo.setKeepLive(channel, variableHeader.keepAliveTimeSeconds());
        ChannelInfo.setClientId(channel, connectMessage.payload().clientIdentifier());
        ChannelInfo.setCleanSessionFlag(channel, variableHeader.isCleanSession());
        CompletableFuture<Void> future = new CompletableFuture<>();
        ChannelInfo.setFuture(channel, ChannelInfo.FUTURE_CONNECT, future);
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        }, 1, TimeUnit.SECONDS);
        String remark = upstreamHookResult.getRemark();
        if (!upstreamHookResult.isSuccess()) {
            byte connAckCode = (byte) upstreamHookResult.getSubCode();
            MqttConnectReturnCode mqttConnectReturnCode = MqttConnectReturnCode.valueOf(connAckCode);
            if (mqttConnectReturnCode == null) {
                channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, remark);
                return;
            }
            channel.writeAndFlush(getMqttConnAckMessage(mqttConnectReturnCode));
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, remark);
            return;
        }
        try {
            MqttConnAckMessage mqttConnAckMessage = getMqttConnAckMessage(MqttConnectReturnCode.CONNECTION_ACCEPTED);
            future.thenAccept(aVoid -> {
                if (!channel.isActive()) {
                    return;
                }
                ChannelInfo.removeFuture(channel, ChannelInfo.FUTURE_CONNECT);
                channel.writeAndFlush(mqttConnAckMessage);
            });
            sessionLoop.loadSession(ChannelInfo.getClientId(channel), channel);
        } catch (Exception e) {
            logger.error("Connect:{}", connectMessage.payload().clientIdentifier(), e);
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "ConnectException");
        }
    }

    private MqttConnAckMessage getMqttConnAckMessage(MqttConnectReturnCode returnCode) {
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(returnCode, false);
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckMessage mqttConnAckMessage =
                new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
        return mqttConnAckMessage;
    }

}
