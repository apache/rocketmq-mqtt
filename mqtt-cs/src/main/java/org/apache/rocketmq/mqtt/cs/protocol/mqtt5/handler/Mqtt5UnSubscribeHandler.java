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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttUnSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class Mqtt5UnSubscribeHandler implements MqttPacketHandler<MqttUnsubscribeMessage> {

    private static Logger logger = LoggerFactory.getLogger(MqttUnSubscribeHandler.class);

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private ChannelManager channelManager;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttUnsubscribeMessage mqttMessage) {
        MqttUnsubscribePayload mqttUnsubscribePayload = mqttMessage.payload();
        Channel channel = ctx.channel();
        if (mqttUnsubscribePayload == null) {
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
    public void doHandler(ChannelHandlerContext ctx, MqttUnsubscribeMessage mqttMessage, HookResult upstreamHookResult) {
        String clientId = ChannelInfo.getClientId(ctx.channel());
        Channel channel = ctx.channel();
        String remark = upstreamHookResult.getRemark();
        if (!upstreamHookResult.isSuccess()) {
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, remark);
            return;
        }
        try {
            MqttUnsubscribePayload payload = mqttMessage.payload();
            if (payload.topics() != null && !payload.topics().isEmpty()) {
                short[] unSubAckCodes = new short[payload.topics().size()];
                int i = 0;
                for (String topic : payload.topics()) {
                    Session session = sessionLoop.getSession(ChannelInfo.getId(channel));
                    if (session.getSubscription(TopicUtils.normalizeTopic(topic)) == null) {
                        unSubAckCodes[i++] = MqttReasonCodes.UnsubAck.NO_SUBSCRIPTION_EXISTED.byteValue();
                    } else {
                        sessionLoop.removeSubscription(ChannelInfo.getId(channel), new Subscription(TopicUtils.normalizeTopic(topic)));
                        unSubAckCodes[i++] = MqttReasonCodes.UnsubAck.SUCCESS.byteValue();
                    }
                }
                int messageId = mqttMessage.variableHeader().messageId();
                sendUnSubAck(channel, messageId, unSubAckCodes);
            }
        } catch (Exception e) {
            logger.error("UnSubscribe:{}", clientId, e);
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "UnSubscribeException");
        }
    }

    public void sendUnSubAck(Channel channel, Integer packetId, short[] unSubAckCodes) {
        channel.writeAndFlush(MqttMessageFactory.createUnSubAckMessage(packetId, unSubAckCodes, MqttProperties.NO_PROPERTIES));
    }
}
