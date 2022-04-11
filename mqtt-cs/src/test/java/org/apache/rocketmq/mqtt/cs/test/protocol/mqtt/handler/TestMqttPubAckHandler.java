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
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubAckHandler;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttPubAckHandler {
    private MqttPubAckHandler pubAckHandler;
    private MqttMessageIdVariableHeader variableHeader;
    private MqttPubAckMessage pubAckMessage;
    private MqttFixedHeader fixedHeader;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private HookResult hookResult;

    @Mock
    private PushAction pushAction;

    @Mock
    private RetryDriver retryDriver;

    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private Session session;

    @Spy
    private NioSocketChannel channel;

    @Before
    public void setUp() throws Exception {
        variableHeader = MqttMessageIdVariableHeader.from(007);
        fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        pubAckMessage = new MqttPubAckMessage(fixedHeader, variableHeader);
    }

    @Test
    public void testDoHandler() throws IllegalAccessException {
        pubAckHandler = new MqttPubAckHandler();
        FieldUtils.writeDeclaredField(pubAckHandler, "pushAction", pushAction, true);
        FieldUtils.writeDeclaredField(pubAckHandler, "retryDriver", retryDriver, true);
        FieldUtils.writeDeclaredField(pubAckHandler, "sessionLoop", sessionLoop, true);

        when(ctx.channel()).thenReturn(channel);
        doReturn(null).when(retryDriver).unMountPublish(anyInt(), anyString());
        doReturn(session).when(sessionLoop).getSession(anyString());
        doNothing().when(pushAction).rollNextByAck(any(), anyInt());

        pubAckHandler.doHandler(ctx, pubAckMessage, hookResult);
        verify(ctx, times(2)).channel();
        verify(sessionLoop).getSession(anyString());
        verify(pushAction).rollNextByAck(eq(session), anyInt());
    }
}
