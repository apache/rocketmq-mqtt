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
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapSubscribeHandler;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.loop.CoapSessionLoop;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapSubscribeHandler {

    private CoapSubscribeHandler coapSubscribeHandler;
    private CoapRequestMessage coapMessage;

    @Mock
    private CoapSessionLoop sessionLoop;

    @Mock
    private RetainedPersistManager retainedPersistManager;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private CoapSession session;

    @Before
    public void setUp() throws IllegalAccessException {
        coapSubscribeHandler = new CoapSubscribeHandler();
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "retainedPersistManager", retainedPersistManager, true);
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "datagramChannelManager", datagramChannelManager, true);
        coapMessage = new CoapRequestMessage(
                1,
                CoapMessageType.CON,
                0,
                CoapMessageCode.GET,
                1111,
                null,
                "TestData".getBytes(StandardCharsets.UTF_8),
                new InetSocketAddress("127.0.0.1", 9675)
        );
        coapMessage.setTopic("topic1/r1");
        coapMessage.setQosLevel(MqttQoS.AT_LEAST_ONCE);
    }

    @Test
    public void testPreHandler() {
        boolean result = coapSubscribeHandler.preHandler(ctx, coapMessage);
        assertTrue(result);
    }

    @Test
    public void testDoHandlerUpstreamFail() {
        HookResult failHookResult = new HookResult(HookResult.FAIL, "Error", null);

        coapSubscribeHandler.doHandler(ctx, coapMessage, failHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.INTERNAL_SERVER_ERROR);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoHanlderOldSession() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        when(sessionLoop.getSession(any(InetSocketAddress.class))).thenReturn(session);

        coapSubscribeHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(sessionLoop).getSession(any(InetSocketAddress.class));
        verify(session).refreshSubscribeTime();
        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.CONTENT);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoHanlderNewSession() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        when(sessionLoop.getSession(any(InetSocketAddress.class))).thenReturn(null);

        coapSubscribeHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(sessionLoop).getSession(any(InetSocketAddress.class));
        verify(sessionLoop).addSession(any(CoapSession.class), any());
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoResponseFail() {
        coapSubscribeHandler.doResponseFail(coapMessage, "Error");

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.INTERNAL_SERVER_ERROR);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoResponseSuccess() {
        coapSubscribeHandler.doResponseSuccess(coapMessage, session);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.CONTENT);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }
}
