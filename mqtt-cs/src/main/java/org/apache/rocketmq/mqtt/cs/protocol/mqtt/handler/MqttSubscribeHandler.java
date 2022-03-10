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

package org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBACK;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;



@Component
public class MqttSubscribeHandler implements MqttPacketHandler<MqttSubscribeMessage> {
    private static Logger logger = LoggerFactory.getLogger(MqttSubscribeHandler.class);


    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private ChannelManager channelManager;

    @Resource
    private ConnectConf connectConf;

    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("check_connect_future"));


    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttSubscribeMessage mqttMessage, HookResult upstreamHookResult) {
        String clientId = ChannelInfo.getClientId(ctx.channel());
        Channel channel = ctx.channel();
        CompletableFuture<Void> future = new CompletableFuture<>();
        ChannelInfo.setFuture(channel, ChannelInfo.FUTURE_SUBSCRIBE, future);
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        },1,TimeUnit.SECONDS);
        String remark = upstreamHookResult.getRemark();
        if (!upstreamHookResult.isSuccess()) {
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, remark);
            return;
        }
        try {
            MqttSubscribePayload payload = mqttMessage.payload();
            List<MqttTopicSubscription> mqttTopicSubscriptions = payload.topicSubscriptions();
            if (mqttTopicSubscriptions != null && !mqttTopicSubscriptions.isEmpty()) {
                Set<Subscription> subscriptions = new HashSet<>(mqttTopicSubscriptions.size());
                for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
                    Subscription subscription = new Subscription();
                    subscription.setQos(mqttTopicSubscription.qualityOfService().value());
                    subscription.setTopicFilter(TopicUtils.normalizeTopic(mqttTopicSubscription.topicName()));
                    subscriptions.add(subscription);
                }
                sessionLoop.addSubscription(ChannelInfo.getId(ctx.channel()), subscriptions);
            }
            future.thenAccept(aVoid -> {
                if (!channel.isActive()) {
                    return;
                }
                ChannelInfo.removeFuture(channel, ChannelInfo.FUTURE_SUBSCRIBE);
                channel.writeAndFlush(getResponse(mqttMessage));
            });
        } catch (Exception e) {
            logger.error("Subscribe:{}", clientId, e);
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "SubscribeException");
        }
    }


    private MqttSubAckMessage getResponse(MqttSubscribeMessage mqttSubscribeMessage) {
        MqttSubscribePayload payload = mqttSubscribeMessage.payload();
        List<MqttTopicSubscription> mqttTopicSubscriptions = payload.topicSubscriptions();
        // AT_MOST_ONCE
        int[] qoss = new int[mqttTopicSubscriptions.size()];
        int i = 0;
        for (MqttTopicSubscription sub : mqttTopicSubscriptions) {
            qoss[i++] = sub.qualityOfService().value();
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(SUBACK, false, AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = from(mqttSubscribeMessage.variableHeader().messageId());
        MqttSubAckMessage mqttSubAckMessage = new MqttSubAckMessage(fixedHeader, variableHeader,
            new MqttSubAckPayload(qoss));
        return mqttSubAckMessage;
    }

}
