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

package org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
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
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
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

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;

@Component
public class Mqtt5SubscribeHandler implements MqttPacketHandler<MqttSubscribeMessage> {
    private static Logger logger = LoggerFactory.getLogger(Mqtt5SubscribeHandler.class);

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private ChannelManager channelManager;

    @Resource
    private ConnectConf connectConf;

    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("check_subscribe_future"));

    private static final Integer SUBSCRIPTION_IDENTIFIER_MAX = 268435455;
    private static final Integer SUBSCRIPTION_IDENTIFIER_MIN = 1;

    private static final Integer SUBSCRIPTION_IDENTIFIER_NO_PRESENT = -1;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttSubscribeMessage mqttMessage) {
        MqttSubscribeMessage mqttSubscribeMessage = mqttMessage;
        final MqttMessageIdAndPropertiesVariableHeader variableHeader =
                (MqttMessageIdAndPropertiesVariableHeader) mqttMessage.variableHeader();
        Channel channel = ctx.channel();

        // check MqttSubscribeMessage
        MqttProperties.IntegerProperty subscriptionIdentifierProperty =
                (MqttProperties.IntegerProperty) variableHeader.properties().getProperty(SUBSCRIPTION_IDENTIFIER.value());
        if (subscriptionIdentifierProperty != null) {
            Integer subscriptionIdentifier = subscriptionIdentifierProperty.value();
            if (subscriptionIdentifier > SUBSCRIPTION_IDENTIFIER_MAX || subscriptionIdentifier < SUBSCRIPTION_IDENTIFIER_MIN) {
                channelManager.closeConnect(
                        channel,
                        ChannelCloseFrom.SERVER,
                        "PROTOCOL_ERROR",
                        MqttReasonCodes.Disconnect.PROTOCOL_ERROR.byteValue());
                return false;
            }
        }

        MqttSubscribePayload mqttSubscribePayload = mqttSubscribeMessage.payload();
        if (mqttSubscribePayload == null) {
            channelManager.closeConnect(
                    channel,
                    ChannelCloseFrom.SERVER,
                    "PROTOCOL_ERROR",
                    MqttReasonCodes.Disconnect.PROTOCOL_ERROR.byteValue());
            return false;
        }


        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttSubscribeMessage mqttMessage, HookResult upstreamHookResult) {
        String clientId = ChannelInfo.getClientId(ctx.channel());
        Channel channel = ctx.channel();
        if (!upstreamHookResult.isSuccess()) {
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, upstreamHookResult.getRemark());
            return;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ChannelInfo.setFuture(channel, ChannelInfo.FUTURE_SUBSCRIBE, future);
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        }, 1, TimeUnit.SECONDS);

        try {
            final MqttMessageIdAndPropertiesVariableHeader variableHeader =
                    (MqttMessageIdAndPropertiesVariableHeader) mqttMessage.variableHeader();
            MqttSubscribePayload payload = mqttMessage.payload();
            List<MqttTopicSubscription> mqttTopicSubscriptions = payload.topicSubscriptions();
            Set<Subscription> subscriptions = new HashSet<>();
            if (mqttTopicSubscriptions != null && !mqttTopicSubscriptions.isEmpty()) {
                for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
                    Subscription subscription = new Subscription();

                    if (mqttTopicSubscription.qualityOfService().value() > 2) {
                        channelManager.closeConnect(
                                channel,
                                ChannelCloseFrom.SERVER,
                                "PROTOCOL_ERROR",
                                MqttReasonCodes.Disconnect.PROTOCOL_ERROR.byteValue());
                        return;
                    }
                    subscription.setQos(mqttTopicSubscription.qualityOfService().value());
                    subscription.setTopicFilter(TopicUtils.normalizeTopic(mqttTopicSubscription.topicName()));
                    subscription.setNoLocal(mqttTopicSubscription.option().isNoLocal());
                    subscription.setRetainAsPublished(mqttTopicSubscription.option().isRetainAsPublished());
                    subscription.setRetainHandling(mqttTopicSubscription.option().retainHandling());

                    MqttProperties.IntegerProperty subscriptionIdentifierProperty =
                            (MqttProperties.IntegerProperty) variableHeader.properties().getProperty(SUBSCRIPTION_IDENTIFIER.value());
                    if (subscriptionIdentifierProperty != null) {
                        subscription.setSubscriptionIdentifier(subscriptionIdentifierProperty.value());
                    } else {
                        subscription.setSubscriptionIdentifier(SUBSCRIPTION_IDENTIFIER_NO_PRESENT);
                    }
                    subscriptions.add(subscription);
                }
                sessionLoop.addSubscription(ChannelInfo.getId(ctx.channel()), subscriptions);
            }

            future.thenAccept(aVoid -> {
                if (!channel.isActive()) {
                    return;
                }
                ChannelInfo.removeFuture(channel, ChannelInfo.FUTURE_SUBSCRIBE);

                int[] reasonCode = new int[mqttTopicSubscriptions.size()];
                int i = 0;
                for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
                    int qos = mqttTopicSubscription.qualityOfService().value();
                    String topic = mqttTopicSubscription.topicName();
                    boolean noLocal = mqttTopicSubscription.option().isNoLocal();

                    if (!connectConf.isEnableSharedSubscription() && TopicUtils.isSharedSubscription(topic)) {
                        reasonCode[i++] = MqttReasonCodes.SubAck.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED.byteValue();
                        continue;
                    }

                    if (qos < 0 || qos > 2 || (noLocal && TopicUtils.isSharedSubscription(topic))) {
                        reasonCode[i++] = MqttReasonCodes.SubAck.UNSPECIFIED_ERROR.byteValue();
                        continue;
                    }

                    reasonCode[i++] = qos;
                }

                sendSubAck(channel, variableHeader.messageId(), reasonCode);
            });
        } catch (Exception e) {
            logger.error("Subscribe:{}", clientId, e);
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "SubscribeException");
        }
    }

    public void sendSubAck(Channel channel, Integer packetId, int[] subAckCodes) {
        channel.writeAndFlush(MqttMessageFactory.createSubAckMessage(packetId, subAckCodes, MqttProperties.NO_PROPERTIES));
    }
}
