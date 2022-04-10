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
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubRecHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttPubRecHandler {

    private MqttPubRecHandler pubRecHandler;

    private final int messageId = 666;
    private final MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(messageId);
    private MqttFixedHeader expectedPubRelFixHeader;

    @Mock
    private RetryDriver retryDriver;

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
        pubRecHandler = new MqttPubRecHandler();
        FieldUtils.writeDeclaredField(pubRecHandler, "retryDriver", retryDriver, true);

        expectedPubRelFixHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
    }

    @Test
    public void testDoHandler() throws Exception {
        when(ctx.channel()).thenReturn(channel);
        when(mqttMessage.variableHeader()).thenReturn(variableHeader);
        doReturn(null).when(channel).writeAndFlush(any(MqttMessage.class));

        pubRecHandler.doHandler(ctx, mqttMessage, hookResult);

        verify(ctx, times(2)).channel();
        verify(mqttMessage).variableHeader();
        verify(retryDriver).unMountPublish(eq(messageId), anyString());
        verify(retryDriver).mountPubRel(eq(messageId), anyString());
        verify(channel).writeAndFlush(any(MqttMessage.class));
        verifyNoMoreInteractions(retryDriver, ctx, mqttMessage);

        // check qosLevel of flushed pub-rel-mqtt-fixed-header
        Field testFixedHeader = pubRecHandler.getClass().getDeclaredField("pubRelMqttFixedHeader");
        testFixedHeader.setAccessible(true);
        Assert.assertEquals(expectedPubRelFixHeader.toString(), testFixedHeader.get(pubRecHandler).toString());
    }
}
