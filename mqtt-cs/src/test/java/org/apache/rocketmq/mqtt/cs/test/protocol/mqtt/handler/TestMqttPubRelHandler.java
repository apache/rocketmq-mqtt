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

package org.apache.rocketmq.mqtt.cs.test.protocol.mqtt.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubRelHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttPubRelHandler {

    private MqttPubRelHandler pubRelHandler;

    private final int messageId = 666;
    private final MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private MqttMessage mqttMessage;

    @Mock
    private HookResult hookResult;

    @Spy
    private NioSocketChannel channel;

    @Before
    public void setUp() throws Exception {
        pubRelHandler = new MqttPubRelHandler();
        FieldUtils.writeDeclaredField(pubRelHandler, "inFlyCache", inFlyCache, true);
    }

    @Test
    public void testDoHandler() {
        when(ctx.channel()).thenReturn(channel);
        when(mqttMessage.variableHeader()).thenReturn(variableHeader);
        doReturn(null).when(channel).writeAndFlush(any(MqttMessage.class));

        pubRelHandler.doHandler(ctx, mqttMessage, hookResult);

        verify(ctx, times(2)).channel();
        verify(mqttMessage).variableHeader();
        verify(inFlyCache).remove(eq(InFlyCache.CacheType.PUB), anyString(), eq(messageId));
        verify(channel).writeAndFlush(any(MqttMessage.class));
        verifyNoMoreInteractions(inFlyCache, ctx, mqttMessage, hookResult);
    }
}
