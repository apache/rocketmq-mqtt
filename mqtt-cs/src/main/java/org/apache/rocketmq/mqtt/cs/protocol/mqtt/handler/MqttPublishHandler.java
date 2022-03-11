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
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class MqttPublishHandler implements MqttPacketHandler<MqttPublishMessage> {
    private static Logger logger = LoggerFactory.getLogger(MqttPublishHandler.class);

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private ChannelManager channelManager;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private ConnectConf connectConf;


    @Override
    public void doHandler(ChannelHandlerContext ctx,
                          MqttPublishMessage mqttMessage,
                          HookResult upstreamHookResult) {
        final MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        Channel channel = ctx.channel();
        String channelId = ChannelInfo.getId(channel);

        if (!upstreamHookResult.isSuccess()) {
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, upstreamHookResult.getRemark());
            return;
        }

        final boolean isQos2Message = isQos2Message(mqttMessage);
        if (isQos2Message) {
            if (inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, variableHeader.messageId())) {
                doResponse(ctx, mqttMessage);
                return;
            }
        }
        doResponse(ctx, mqttMessage);
        if (isQos2Message) {
            inFlyCache.put(InFlyCache.CacheType.PUB, channelId, variableHeader.messageId());
        }
    }

    private boolean isQos2Message(MqttPublishMessage mqttPublishMessage) {
        return MqttQoS.EXACTLY_ONCE.equals(mqttPublishMessage.fixedHeader().qosLevel());
    }

    private void doResponse(ChannelHandlerContext ctx, MqttPublishMessage mqttMessage) {
        MqttFixedHeader fixedHeader = mqttMessage.fixedHeader();
        MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        switch (fixedHeader.qosLevel()) {
            case AT_MOST_ONCE:
                break;
            case AT_LEAST_ONCE:
                MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false,
                    MqttQoS.AT_MOST_ONCE,
                    false, 0);
                MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader
                    .from(variableHeader.messageId());
                MqttPubAckMessage pubackMessage = new MqttPubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
                ctx.channel().writeAndFlush(pubackMessage);
                break;
            case EXACTLY_ONCE:
                MqttFixedHeader pubrecMqttHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false,
                    MqttQoS.AT_MOST_ONCE,
                    false, 0);
                MqttMessageIdVariableHeader pubrecMessageIdVariableHeader = MqttMessageIdVariableHeader
                    .from(variableHeader.messageId());
                MqttMessage pubrecMqttMessage = new MqttMessage(pubrecMqttHeader, pubrecMessageIdVariableHeader);
                ctx.channel().writeAndFlush(pubrecMqttMessage);
                break;
            default:
                throw new IllegalArgumentException("unknown qos:" + fixedHeader.qosLevel());
        }
    }

}
