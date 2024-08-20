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
package org.apache.rocketmq.mqtt.cs.test.protocol.coap.handler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapRequestType;
import org.apache.rocketmq.mqtt.common.util.CoapTokenUtil;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapDisconnectHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


@RunWith(MockitoJUnitRunner.class)
public class TestCoapDisconnectHandler {

    private CoapDisconnectHandler coapDisconnectHandler;
    private CoapRequestMessage coapMessage;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void setUp() throws Exception {
        coapDisconnectHandler = new CoapDisconnectHandler();
        FieldUtils.writeDeclaredField(coapDisconnectHandler, "datagramChannelManager", datagramChannelManager, true);
        coapMessage = new CoapRequestMessage(
                Constants.COAP_VERSION,
                CoapMessageType.CON,
                0,
                CoapMessageCode.DELETED,
                1111,
                null,
                null,
                new InetSocketAddress("127.0.0.1", 9675)
        );
        coapMessage.setRequestType(CoapRequestType.DISCONNECT);
        coapMessage.setClientId("123");
        coapMessage.setAuthToken(CoapTokenUtil.generateToken("123"));
    }

    @Test
    public void testPreHandler() {
        assertTrue(coapDisconnectHandler.preHandler(ctx, coapMessage));
        verifyNoMoreInteractions(datagramChannelManager, ctx);

        coapMessage.setClientId(null);
        assertFalse(coapDisconnectHandler.preHandler(ctx, coapMessage));
        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.BAD_REQUEST, response.getCode());
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }

    @Test
    public void testDisconnectFail() {
        HookResult failHookResult = new HookResult(HookResult.FAIL, "Error", null);

        coapDisconnectHandler.doHandler(ctx, coapMessage, failHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.INTERNAL_SERVER_ERROR, response.getCode());
            assertEquals(failHookResult.getRemark(), new String(response.getPayload(), StandardCharsets.UTF_8));
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }

    @Test
    public void testDisconnectUnauthorized() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        coapMessage.setAuthToken("wrongToken");

        coapDisconnectHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.UNAUTHORIZED, response.getCode());
            assertEquals("AuthToken is not valid.", new String(response.getPayload(), StandardCharsets.UTF_8));
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }

    @Test
    public void testDisconnectSuccess() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);

        coapDisconnectHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.DELETED, response.getCode());
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }
}
