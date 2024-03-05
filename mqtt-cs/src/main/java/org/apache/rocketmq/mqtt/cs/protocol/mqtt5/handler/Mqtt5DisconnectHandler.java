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
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REASON_STRING;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttReasonCodes.Disconnect.PROTOCOL_ERROR;

@Component
public class Mqtt5DisconnectHandler implements MqttPacketHandler<MqttMessage> {

    private static Logger logger = LoggerFactory.getLogger(Mqtt5DisconnectHandler.class);


    @Resource
    private ChannelManager channelManager;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage) {
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttMessage mqttMessage, HookResult upstreamHookResult) {
        final MqttReasonCodeAndPropertiesVariableHeader variableHeader = (MqttReasonCodeAndPropertiesVariableHeader) mqttMessage.variableHeader();

        if (variableHeader == null) {
            channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.CLIENT, "disconnect");
            return;
        }

        if (variableHeader.properties() != null && variableHeader.properties().getProperty(SESSION_EXPIRY_INTERVAL.value()) != null) {
            Integer disconnectSessionExpiryInterval = (Integer) variableHeader.properties().getProperty(SESSION_EXPIRY_INTERVAL.value()).value();
            Integer channelSessionExpiryInterval = ChannelInfo.getSessionExpiryInterval(ctx.channel());
            if (channelSessionExpiryInterval != null && disconnectSessionExpiryInterval != null) {
                if (channelSessionExpiryInterval == 0) {
                    // The Server uses DISCONNECT with Reason Code 0x82 (Protocol Error)
                    ctx.channel().writeAndFlush(MqttMessageFactory.createDisconnectMessage(PROTOCOL_ERROR.byteValue()));
                } else {
                    ChannelInfo.setSessionExpiryInterval(ctx.channel(), disconnectSessionExpiryInterval);
                }
            }
        }

        // only process reason code witch send by client
        switch (MqttReasonCodes.Disconnect.valueOf(variableHeader.reasonCode())) {
            case NORMAL_DISCONNECT:
                // do not send will message
                channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.CLIENT, "disconnect");
                break;
            case DISCONNECT_WITH_WILL_MESSAGE:
                // send will message
                channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.CLIENT, "disconnect_with_will");
                break;
            default:
                channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.CLIENT, MqttReasonCodes.Disconnect.valueOf(variableHeader.reasonCode()).name());
        }

        if (variableHeader.properties().getProperty(REASON_STRING.value()) != null) {
            logger.info("{} disconnect reason string: {}", ChannelInfo.getClientId(ctx.channel()), variableHeader.properties().getProperty(REASON_STRING.value()).value());
        }
    }
}
