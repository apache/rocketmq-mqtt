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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapAckHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapRetryManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapAckHandler {

    private CoapAckHandler coapAckHandler;

    @Mock
    private CoapRetryManager coapRetryManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private ConnectConf connectConf;

    private CoapRequestMessage coapMessage;

    @Before
    public void setUp() throws IllegalAccessException {
        coapAckHandler = new CoapAckHandler();
        FieldUtils.writeDeclaredField(coapAckHandler, "coapRetryManager", coapRetryManager, true);
        FieldUtils.writeDeclaredField(coapAckHandler, "connectConf", connectConf, true);
        coapMessage = new CoapRequestMessage(
                1,
                CoapMessageType.ACK,
                0,
                CoapMessageCode.EMPTY,
                1111,
                null,
                null,
                new InetSocketAddress("127.0.0.1", 9675)
        );
    }

    @Test
    public void testPreHandler() {
        when(connectConf.isEnableCoapConnect()).thenReturn(false);
        boolean result = coapAckHandler.preHandler(ctx, coapMessage);
        assertTrue(result);
    }

    @Test
    public void testAckSuccess() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        when(coapRetryManager.contains(anyInt())).thenReturn(true);

        coapAckHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(coapRetryManager).contains(anyInt());
        verify(coapRetryManager).ackMessage(anyInt());
        verifyNoMoreInteractions(ctx, coapRetryManager, connectConf);
    }
}
