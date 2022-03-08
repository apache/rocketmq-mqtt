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


import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;



@Component
public class MqttPubAckHandler implements MqttPacketHandler<MqttPubAckMessage> {
    private static Logger logger = LoggerFactory.getLogger(MqttPubAckHandler.class);

    @Resource
    private PushAction pushAction;

    @Resource
    private RetryDriver retryDriver;

    @Resource
    private SessionLoop sessionLoop;

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttPubAckMessage mqttMessage, HookResult upstreamHookResult) {
        int messageId = mqttMessage.variableHeader().messageId();
        retryDriver.unMountPublish(messageId, ChannelInfo.getId(ctx.channel()));
        pushAction.rollNextByAck(sessionLoop.getSession(ChannelInfo.getId(ctx.channel())), messageId);
    }
}
