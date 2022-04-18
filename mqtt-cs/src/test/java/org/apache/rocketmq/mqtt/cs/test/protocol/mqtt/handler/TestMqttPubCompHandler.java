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
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubCompHandler;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttPubCompHandler {
    private MqttPubCompHandler pubCompHandler;

    @Mock
    private RetryDriver retryDriver;

    @Mock
    private PushAction pushAction;

    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private MqttMessage mqttMessage;

    @Mock
    private HookResult hookResult;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private Session session;

    @Test
    public void testDoHandler() throws IllegalAccessException {
        pubCompHandler = new MqttPubCompHandler();
        FieldUtils.writeDeclaredField(pubCompHandler, "retryDriver", retryDriver, true);
        FieldUtils.writeDeclaredField(pubCompHandler, "pushAction", pushAction, true);
        FieldUtils.writeDeclaredField(pubCompHandler, "sessionLoop", sessionLoop, true);

        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(666);
        when(mqttMessage.variableHeader()).thenReturn(variableHeader);
        when(ctx.channel()).thenReturn(channel);
        when(sessionLoop.getSession(any())).thenReturn(session);

        pubCompHandler.doHandler(ctx, mqttMessage, hookResult);
        verify(mqttMessage).variableHeader();
        verify(ctx, times(2)).channel();
        verify(retryDriver).unMountPubRel(anyInt(), anyString());
        verify(sessionLoop).getSession(anyString());
        verify(pushAction).rollNextByAck(eq(session), anyInt());
    }
}
