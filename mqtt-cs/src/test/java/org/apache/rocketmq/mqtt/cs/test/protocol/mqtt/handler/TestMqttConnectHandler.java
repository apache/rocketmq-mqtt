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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttConnectHandler;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttConnectHandler {
    private MqttConnectHandler connectHandler;
    private MqttConnectMessage connectMessage;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DefaultChannelManager channelManager;

    @Mock
    private SessionLoop sessionLoop;

    @Before
    public void setUp() throws IllegalAccessException {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(null, 0, false,
            false, false, 0, false, true, 1);
        MqttConnectPayload payload = new MqttConnectPayload("testConnHandler", null, (byte[]) null, null, null);
        connectMessage = new MqttConnectMessage(mqttFixedHeader, variableHeader, payload);

        connectHandler = new MqttConnectHandler();
        FieldUtils.writeDeclaredField(connectHandler, "channelManager", channelManager, true);
        FieldUtils.writeDeclaredField(connectHandler, "sessionLoop", sessionLoop, true);

        when(ctx.channel()).thenReturn(channel);
    }

    @After
    public void After() {}

    @Test
    public void testDoHandlerAuthFailed() {
        HookResult authFailHook = new HookResult(HookResult.FAIL,
            MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
        doReturn(null).when(channel).writeAndFlush(any());
        doNothing().when(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);

        connectHandler.doHandler(ctx, connectMessage, authFailHook);

        verify(channel).writeAndFlush(any());
        verify(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);
        verifyNoMoreInteractions(channelManager, sessionLoop);
    }

    @Test
    public void testDoHandlerChannelInActive() {
        HookResult hookResult = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);
        doReturn(false).when(channel).isActive();
        doNothing().when(sessionLoop).loadSession(any(), any());

        connectHandler.doHandler(ctx, connectMessage, hookResult);

        // wait scheduler execution
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}

        verify(sessionLoop).loadSession(any(), any());
        verifyNoMoreInteractions(channelManager, sessionLoop);
    }

    @Test
    public void testDoHandlerSuccess() {
        HookResult hookResult = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);
        doReturn(true).when(channel).isActive();
        doNothing().when(sessionLoop).loadSession(any(), any());

        connectHandler.doHandler(ctx, connectMessage, hookResult);

        // wait scheduler execution
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}

        verify(channel).writeAndFlush(any(MqttConnAckMessage.class));
        verify(sessionLoop).loadSession(any(), any());
        verifyNoMoreInteractions(channelManager, sessionLoop);
    }
}
