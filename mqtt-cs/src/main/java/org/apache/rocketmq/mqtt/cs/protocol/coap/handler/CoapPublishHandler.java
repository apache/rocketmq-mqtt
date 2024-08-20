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
package org.apache.rocketmq.mqtt.cs.protocol.coap.handler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.util.CoapTokenUtil;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.CoapPacketHandler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

@Component
public class CoapPublishHandler implements CoapPacketHandler<CoapRequestMessage> {

    @Resource
    private DatagramChannelManager datagramChannelManager;

    @Resource
    private ConnectConf connectConf;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        if (connectConf.isEnableCoapConnect()) {
            if (coapMessage.getClientId() == null || coapMessage.getAuthToken() == null) {
                CoapMessage response = new CoapMessage(
                        Constants.COAP_VERSION,
                        coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                        coapMessage.getTokenLength(),
                        CoapMessageCode.BAD_REQUEST,
                        coapMessage.getMessageId(),
                        coapMessage.getToken(),
                        "Not complete info for connection mode.".getBytes(StandardCharsets.UTF_8),
                        coapMessage.getRemoteAddress()
                );
                datagramChannelManager.writeResponse(response);
                return false;
            }
            if (!CoapTokenUtil.isValid(coapMessage.getClientId(), coapMessage.getAuthToken())) {
                CoapMessage response = new CoapMessage(
                        Constants.COAP_VERSION,
                        coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                        coapMessage.getTokenLength(),
                        CoapMessageCode.UNAUTHORIZED,
                        coapMessage.getMessageId(),
                        coapMessage.getToken(),
                        "Invalid authToken.".getBytes(StandardCharsets.UTF_8),
                        coapMessage.getRemoteAddress()
                );
                datagramChannelManager.writeResponse(response);
                return false;
            }
        }
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, HookResult upstreamHookResult) {
        // Send response to client with success/error content.
        CoapMessage response;
        if (upstreamHookResult.isSuccess()) {
            response = new CoapMessage(
                    Constants.COAP_VERSION,
                    coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                    coapMessage.getTokenLength(),
                    CoapMessageCode.CREATED,
                    coapMessage.getMessageId(),
                    coapMessage.getToken(),
                    null,
                    coapMessage.getRemoteAddress()
            );
        } else {
            response = new CoapMessage(
                    Constants.COAP_VERSION,
                    coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                    coapMessage.getTokenLength(),
                    CoapMessageCode.INTERNAL_SERVER_ERROR,
                    coapMessage.getMessageId(),
                    coapMessage.getToken(),
                    upstreamHookResult.getRemark().getBytes(),
                    coapMessage.getRemoteAddress()
            );
        }
        datagramChannelManager.writeResponse(response);
    }
}
