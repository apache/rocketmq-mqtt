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
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapConnectHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapConnectHandler {

    private CoapConnectHandler coapConnectHandler;
    private CoapRequestMessage coapMessage;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void setUp() throws Exception {
        coapConnectHandler = new CoapConnectHandler();
        FieldUtils.writeDeclaredField(coapConnectHandler, "datagramChannelManager", datagramChannelManager, true);
        coapMessage = new CoapRequestMessage(
                Constants.COAP_VERSION,
                CoapMessageType.CON,
                0,
                CoapMessageCode.POST,
                1111,
                null,
                null,
                new InetSocketAddress("127.0.0.1", 9675)
        );
        coapMessage.setRequestType(CoapRequestType.CONNECT);
        coapMessage.setClientId("123");
        coapMessage.setUserName("admin");
        coapMessage.setPassword("public");
    }

    @Test
    public void testPreHandler() {
        assertTrue(coapConnectHandler.preHandler(ctx, coapMessage));
        verifyNoMoreInteractions(datagramChannelManager, ctx);

        coapMessage.setClientId(null);
        assertFalse(coapConnectHandler.preHandler(ctx, coapMessage));
        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.BAD_REQUEST, response.getCode());
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }

    @Test
    public void testConnectFail() {
        HookResult failHookResult = new HookResult(HookResult.FAIL, "Error", null);

        coapConnectHandler.doHandler(ctx, coapMessage, failHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.UNAUTHORIZED, response.getCode());
            assertEquals(failHookResult.getRemark(), new String(response.getPayload(), StandardCharsets.UTF_8));
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }

    @Test
    public void testConnectSuccess() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);

        coapConnectHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(CoapMessageCode.CREATED, response.getCode());
            try {
                assertTrue(CoapTokenUtil.isValid(coapMessage.getClientId(), new String(response.getPayload(), StandardCharsets.UTF_8)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }));
        verifyNoMoreInteractions(datagramChannelManager, ctx);
    }
}