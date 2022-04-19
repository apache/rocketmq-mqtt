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
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttUnSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttUnSubscribeHandler {
    private final int messageId = 666;
    private final MqttMessageIdVariableHeader idVariableHeader = MqttMessageIdVariableHeader.from(messageId);
    private final String topicFilter = "test/unSubscribe/#/";

    private MqttFixedHeader mqttFixedHeader;
    private MqttUnsubscribePayload unsubscribePayload;
    private MqttUnsubscribeMessage unsubscribeMessage;
    private List<String> topicList;

    private MqttUnSubscribeHandler unSubscribeHandler;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DefaultChannelManager channelManager;

    @Mock
    private SessionLoop sessionLoop;

    @Before
    public void setUp() throws Exception {
        mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_MOST_ONCE, false, 0);
        topicList = new ArrayList<>();
        topicList.add(topicFilter);
        unsubscribePayload = new MqttUnsubscribePayload(topicList);
        unsubscribeMessage = new MqttUnsubscribeMessage(mqttFixedHeader, idVariableHeader, unsubscribePayload);

        unSubscribeHandler = new MqttUnSubscribeHandler();
        FieldUtils.writeDeclaredField(unSubscribeHandler, "channelManager", channelManager, true);
        FieldUtils.writeDeclaredField(unSubscribeHandler, "sessionLoop", sessionLoop, true);

        when(ctx.channel()).thenReturn(channel);
    }

    @Test
    public void testDoHandlerAuthFailed() {
        HookResult authFailHook = new HookResult(HookResult.FAIL,
                MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
        doNothing().when(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);

        unSubscribeHandler.doHandler(ctx, unsubscribeMessage, authFailHook);

        verify(ctx, times(2)).channel();
        verify(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testDoHandlerSuccess() {
        HookResult hookResult = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);
        doNothing().when(sessionLoop).removeSubscription(anyString(), anySet());
        doReturn(null).when(channel).writeAndFlush(any(MqttUnsubAckMessage.class));

        unSubscribeHandler.doHandler(ctx, unsubscribeMessage, hookResult);

        verify(ctx, times(3)).channel();
        verify(sessionLoop).removeSubscription(anyString(), anySet());
        verify(channel).writeAndFlush(any(MqttUnsubAckMessage.class));
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

}