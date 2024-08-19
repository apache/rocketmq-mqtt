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
import org.apache.rocketmq.mqtt.cs.protocol.CoapPacketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;

@Component
public class CoapHeartbeatHandler implements CoapPacketHandler<CoapRequestMessage> {
    private static Logger logger = LoggerFactory.getLogger(CoapHeartbeatHandler.class);

    @Resource
    private DatagramChannelManager datagramChannelManager;

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        if (coapMessage.getClientId() == null || coapMessage.getAuthToken() == null) {
            return false;
        }
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, HookResult upstreamHookResult) {
        // Response fail ack if upstream hook fail.
        if (!upstreamHookResult.isSuccess()) {
            CoapMessage response = new CoapMessage(
                    Constants.COAP_VERSION,
                    CoapMessageType.ACK,
                    coapMessage.getTokenLength(),
                    CoapMessageCode.INTERNAL_SERVER_ERROR,
                    coapMessage.getMessageId(),
                    coapMessage.getToken(),
                    upstreamHookResult.getRemark().getBytes(StandardCharsets.UTF_8),
                    coapMessage.getRemoteAddress()
            );
            datagramChannelManager.writeResponse(response);
            return;
        }
        // Response unauthorized ack if authToken is not valid.
        if (!CoapTokenUtil.isValid(coapMessage.getClientId(), coapMessage.getAuthToken())) {
            CoapMessage response = new CoapMessage(
                    Constants.COAP_VERSION,
                    CoapMessageType.ACK,
                    coapMessage.getTokenLength(),
                    CoapMessageCode.UNAUTHORIZED,
                    coapMessage.getMessageId(),
                    coapMessage.getToken(),
                    "AuthToken is not valid.".getBytes(StandardCharsets.UTF_8),
                    coapMessage.getRemoteAddress()
            );
            datagramChannelManager.writeResponse(response);
            return;
        }
        try {
            // Refresh update time of token, and return new token
            String newAuthToken = CoapTokenUtil.generateToken(coapMessage.getClientId());
            // Response ack success and new token
            CoapMessage response = new CoapMessage(
                    Constants.COAP_VERSION,
                    CoapMessageType.ACK,
                    coapMessage.getTokenLength(),
                    CoapMessageCode.CHANGED,
                    coapMessage.getMessageId(),
                    coapMessage.getToken(),
                    newAuthToken.getBytes(StandardCharsets.UTF_8),
                    coapMessage.getRemoteAddress()
            );
            datagramChannelManager.writeResponse(response);
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
