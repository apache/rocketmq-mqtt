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

package org.apache.rocketmq.mqtt.cs.test.protocol.mqtt5.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5SubscribeHandler;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttSubscriptionOption.RetainedHandlingPolicy.SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS;
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
public class TestMqtt5SubscribeHandler {
    private final String topicFilter = "test/subscribe/#/";

    private Mqtt5SubscribeHandler subscribeHandler;
    private MqttSubscribeMessage subscribeMessage;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DefaultChannelManager channelManager;

    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private ConnectConf connectConf;

    @Before
    public void setUp() throws Exception {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(PAYLOAD_FORMAT_INDICATOR.value(), 6));
        subscribeMessage = MqttMessageBuilders.subscribe()
                .messageId((short) 666)
                .properties(props)
                .addSubscription(topicFilter, new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .build();

        subscribeHandler = new Mqtt5SubscribeHandler();
        FieldUtils.writeDeclaredField(subscribeHandler, "channelManager", channelManager, true);
        FieldUtils.writeDeclaredField(subscribeHandler, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(subscribeHandler, "connectConf", connectConf, true);

        when(ctx.channel()).thenReturn(channel);
        doReturn(null).when(channel).writeAndFlush(any());
    }

    @Test
    public void testPreHandler() {
        subscribeHandler.preHandler(ctx, subscribeMessage);

        verify(ctx, times(1)).channel();
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testPreHandlerWithProtocolError() {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 268435456));
        final MqttSubscribeMessage message = MqttMessageBuilders.subscribe()
                .messageId((short) 666)
                .properties(props)
                .addSubscription(topicFilter, new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .build();

        Assert.assertFalse(subscribeHandler.preHandler(ctx, message));

        verify(ctx, times(1)).channel();
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testPreHandlerWithNoLocalAndShareSub() {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 1));
        final MqttSubscribeMessage message = MqttMessageBuilders.subscribe()
                .messageId((short) 666)
                .properties(props)
                .addSubscription(topicFilter, new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .addSubscription("$share/topic1/test", new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .build();

        Assert.assertFalse(subscribeHandler.preHandler(ctx, message));
        verify(ctx, times(1)).channel();
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testDoHandlerAuthFailed() {
        HookResult authFailHook = new HookResult(HookResult.FAIL,
            MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
        doNothing().when(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);

        subscribeHandler.doHandler(ctx, subscribeMessage, authFailHook);

        verify(ctx, times(2)).channel();
        verify(channelManager).closeConnect(channel, ChannelCloseFrom.SERVER, Remark.AUTH_FAILED);
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testDoHandlerChannelInActive() {
        HookResult hookResult = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);
        doReturn(false).when(channel).isActive();
        doNothing().when(sessionLoop).addSubscription(anyString(), anySet());

        subscribeHandler.doHandler(ctx, subscribeMessage, hookResult);

        // wait scheduler execution
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        verify(ctx, times(3)).channel();
        verify(sessionLoop).addSubscription(anyString(), anySet());
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

    @Test
    public void testDoHandlerWithSharedSub() {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 1));
        final MqttSubscribeMessage message = MqttMessageBuilders.subscribe()
                .messageId((short) 666)
                .properties(props)
                .addSubscription(topicFilter, new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .addSubscription("$share/topic1/test", new MqttSubscriptionOption(AT_LEAST_ONCE,
                        true,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .addSubscription("$share/topic1/test1", new MqttSubscriptionOption(AT_LEAST_ONCE,
                        false,
                        true,
                        SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS))
                .build();

        HookResult hookResult = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);
        doReturn(true).when(channel).isActive();
        doNothing().when(sessionLoop).addSubscription(anyString(), anySet());

        subscribeHandler.doHandler(ctx, message, hookResult);

        // wait scheduler execution
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        verify(ctx, times(3)).channel();
        verify(sessionLoop).addSubscription(anyString(), anySet());
        verify(channel).writeAndFlush(any(MqttSubAckMessage.class));
        verifyNoMoreInteractions(channelManager, sessionLoop, ctx);
    }

}
