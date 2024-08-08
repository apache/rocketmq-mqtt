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
package org.apache.rocketmq.mqtt.cs.test.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.CoapRequestType;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapPacketDispatcher;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapPublishHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapResponseCache;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapPublishProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapSubscribeProcessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapPacketDispatcher {

    @InjectMocks
    private CoapPacketDispatcher coapPacketDispatcher;

    private EmbeddedChannel channel;

    @Mock
    private CoapRequestMessage msg;

    @Mock
    private CoapResponseCache coapResponseCache;

    @Mock
    private CoapMessage oldResponse;

    @Mock
    private CoapPublishHandler coapPublishHandler;

    @Mock
    private CoapPublishProcessor coapPublishProcessor;

    @Mock
    private CoapSubscribeHandler coapSubscribeHandler;

    @Mock
    private CoapSubscribeProcessor coapSubscribeProcessor;

    @Before
    public void setUp() throws IllegalAccessException {
        channel = new EmbeddedChannel(coapPacketDispatcher);
//        FieldUtils.writeDeclaredField(coapPacketDispatcher, "coapResponseCache", coapResponseCache, true);


    }

    @Test
    public void testRead0Retransmit() {
        when(msg.getMessageId()).thenReturn(1);
        when(coapResponseCache.get(anyInt())).thenReturn(oldResponse);
        // Pass msg into channel and invoke channelRead0()
        channel.writeInbound(msg);
        // Assert that the response is written to the channel
        verify(coapResponseCache).get(1);
        Object out = channel.readOutbound();
        assertEquals(oldResponse, out);
        assertNull(channel.readOutbound());
    }

    @Test
    public void testRead0Publish() {
        when(msg.getRequestType()).thenReturn(CoapRequestType.PUBLISH);
        when(coapPublishHandler.preHandler(any(ChannelHandlerContext.class), any(CoapRequestMessage.class))).thenReturn(true);
        CompletableFuture<HookResult> processResult = new CompletableFuture<>();
        processResult.complete(new HookResult(HookResult.SUCCESS, null, null));
        when(coapPublishProcessor.process(any(CoapRequestMessage.class))).thenReturn(processResult);

        channel.writeInbound(msg);

        verify(coapPublishHandler).preHandler(any(ChannelHandlerContext.class), eq(msg));
        verify(coapPublishProcessor).process(msg);
        verify(coapPublishHandler).doHandler(any(ChannelHandlerContext.class), eq(msg), any(HookResult.class));
        assertNull(channel.readOutbound());
    }

    @Test
    public void testRead0Subscribe() {
        when(msg.getRequestType()).thenReturn(CoapRequestType.SUBSCRIBE);
        when(coapSubscribeHandler.preHandler(any(ChannelHandlerContext.class), any(CoapRequestMessage.class))).thenReturn(true);
        CompletableFuture<HookResult> processResult = new CompletableFuture<>();
        processResult.complete(new HookResult(HookResult.SUCCESS, null, null));
        when(coapSubscribeProcessor.process(any(CoapRequestMessage.class))).thenReturn(processResult);

        channel.writeInbound(msg);

        verify(coapSubscribeHandler).preHandler(any(ChannelHandlerContext.class), eq(msg));
        verify(coapSubscribeProcessor).process(msg);
        verify(coapSubscribeHandler).doHandler(any(ChannelHandlerContext.class), eq(msg), any(HookResult.class));
        assertNull(channel.readOutbound());
    }
}
