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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class MqttPubRecHandler implements MqttPacketHandler<MqttMessage> {
    private final MqttFixedHeader pubRelMqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false,
            MqttQoS.AT_LEAST_ONCE, false, 0);

    @Resource
    private RetryDriver retryDriver;

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage, HookResult upstreamHookResult) {
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        String channelId = ChannelInfo.getId(ctx.channel());
        retryDriver.unMountPublish(variableHeader.messageId(), channelId);
        retryDriver.mountPubRel(variableHeader.messageId(), channelId);

        MqttMessage pubRelMqttMessage = new MqttMessage(pubRelMqttFixedHeader, variableHeader);
        ctx.channel().writeAndFlush(pubRelMqttMessage);
    }
}
