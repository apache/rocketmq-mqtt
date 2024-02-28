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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class Mqtt5PubCompHandler implements MqttPacketHandler<MqttMessage> {

    @Resource
    private RetryDriver retryDriver;

    @Resource
    private PushAction pushAction;

    @Resource
    private SessionLoop sessionLoop;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage, HookResult upstreamHookResult) {
        final MqttPubReplyMessageVariableHeader variableHeader = (MqttPubReplyMessageVariableHeader) mqttMessage.variableHeader();
        String channelId = ChannelInfo.getId(ctx.channel());

        retryDriver.unMountPubRel(variableHeader.messageId(), ChannelInfo.getId(ctx.channel()));

        //The Packet Identifier becomes available for reuse once the Sender has received the PUBCOMP Packet.
        Session session = sessionLoop.getSession(channelId);
        pushAction.rollNextByAck(session, variableHeader.messageId());
    }
}
