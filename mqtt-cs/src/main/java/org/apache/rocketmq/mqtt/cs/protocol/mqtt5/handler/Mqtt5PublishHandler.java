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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import java.nio.charset.StandardCharsets;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RETAIN_AVAILABLE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;

@Component
public class Mqtt5PublishHandler implements MqttPacketHandler<MqttPublishMessage> {

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private ChannelManager channelManager;

    @Resource
    private ConnectConf connectConf;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage) {
        final MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        Channel channel = ctx.channel();
        String channelId = ChannelInfo.getId(channel);

        if (inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, variableHeader.packetId())) {
            if (isQos1Message(mqttMessage)) {
                sendPubAck(channel, variableHeader.packetId(), MqttReasonCodes.PubAck.PACKET_IDENTIFIER_IN_USE);
                return false;
            } else if (isQos2Message(mqttMessage)) {
                sendPubRec(channel, variableHeader.packetId(), MqttReasonCodes.PubRec.PACKET_IDENTIFIER_IN_USE);
                return false;
            }
        }

        MqttProperties mqttProperties = variableHeader.properties();
        if (mqttProperties != null) {
            Integer payloadFormatIndicator = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(PAYLOAD_FORMAT_INDICATOR.value())).value();
            if (payloadFormatIndicator != null && payloadFormatIndicator == 1) {
                if (mqttMessage.payload().readableBytes() != 0) {
                    try {
                        mqttMessage.payload().toString(StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        if (isQos1Message(mqttMessage)) {
                            sendPubAck(channel, variableHeader.packetId(), MqttReasonCodes.PubAck.PAYLOAD_FORMAT_INVALID);
                        } else if (isQos2Message(mqttMessage)) {
                            sendPubRec(channel, variableHeader.packetId(), MqttReasonCodes.PubRec.PAYLOAD_FORMAT_INVALID);
                        }
                        return false;
                    }
                }
            }

            if (mqttProperties.getProperty(TOPIC_ALIAS.value()) == null &&
                    (variableHeader.topicName() == null || variableHeader.topicName().isEmpty())) {
                channelManager.closeConnectWithProtocolError(channel);
            }
        }

        if (mqttProperties.getProperty(RETAIN_AVAILABLE.value()) != null &&
                ((MqttProperties.IntegerProperty) mqttProperties.getProperty(RETAIN_AVAILABLE.value())).value() == 1 &&
                !connectConf.isEnableRetain()) {

            final MqttConnAckMessage mqttConnAckMessageRetainNotSupport = MqttMessageFactory.createConnAckMessage(
                    MqttConnectReturnCode.CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED,
                    false,
                    new MqttProperties());
            channel.writeAndFlush(mqttConnAckMessageRetainNotSupport);
            return false;
        }

        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage, HookResult upstreamHookResult) {
        final MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        Channel channel = ctx.channel();
        String channelId = ChannelInfo.getId(channel);

        if (!upstreamHookResult.isSuccess()) {
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, upstreamHookResult.getRemark());
            return;
        }

        doResponseSuccess(ctx, mqttMessage);

        final boolean isQos2Message = isQos2Message(mqttMessage);
        if (isQos2Message && !inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, variableHeader.packetId())) {
            inFlyCache.put(InFlyCache.CacheType.PUB, channelId, variableHeader.packetId());
        }
    }

    private boolean isQos2Message(MqttPublishMessage mqttPublishMessage) {
        return MqttQoS.EXACTLY_ONCE.equals(mqttPublishMessage.fixedHeader().qosLevel());
    }

    private boolean isQos1Message(MqttPublishMessage mqttPublishMessage) {
        return MqttQoS.AT_LEAST_ONCE.equals(mqttPublishMessage.fixedHeader().qosLevel());
    }

    private void doResponseSuccess(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage) {
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        int messageId = variableHeader.packetId();
        switch (fixedHeader.qosLevel()) {
            case AT_MOST_ONCE:
                break;
            case AT_LEAST_ONCE:
                ctx.channel().writeAndFlush(MqttMessageFactory.createPubAckMessage(messageId, MqttReasonCodes.PubAck.SUCCESS.byteValue(), MqttProperties.NO_PROPERTIES));
                break;
            case EXACTLY_ONCE:
                ctx.channel().writeAndFlush(MqttMessageFactory.createPubRecMessage(messageId, MqttReasonCodes.PubAck.SUCCESS.byteValue(), MqttProperties.NO_PROPERTIES));
                break;
            default:
                throw new IllegalArgumentException("unknown qos:" + fixedHeader.qosLevel());
        }
    }

    private void sendPubAck(Channel channel, Integer messageId, MqttReasonCodes.PubAck reasonCode) {
        channel.writeAndFlush(MqttMessageFactory.createPubAckMessage(
                messageId,
                reasonCode.byteValue(),
                MqttProperties.NO_PROPERTIES));
    }

    private void sendPubRec(Channel channel, Integer messageId, MqttReasonCodes.PubRec reasonCode) {
        channel.writeAndFlush(MqttMessageFactory.createPubRecMessage(
                messageId,
                reasonCode.byteValue(),
                MqttProperties.NO_PROPERTIES));
    }
}
