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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttConnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5ConnectHandler;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.AUTHENTICATION_METHOD;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttConnectHandler {
    private Mqtt5ConnectHandler connectHandler;
    private MqttConnectMessage connectMessage;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DefaultChannelManager channelManager;

    @Mock
    private SessionLoop sessionLoop;

    private static final String CLIENT_ID = "RANDOM_TEST_CLIENT";
    private static final String WILL_TOPIC = "/my_will";
    private static final String WILL_MESSAGE = "gone";
    private static final String USER_NAME = "hello";
    private static final String PASSWORD = "world";
    private static final int KEEP_ALIVE_SECONDS = 600;

    private MqttConnectMessage createConnectMessage(MqttVersion mqttVersion,
                                                           String username,
                                                           String password,
                                                           MqttProperties properties,
                                                           MqttProperties willProperties) {
        return MqttMessageBuilders.connect()
                .clientId(CLIENT_ID)
                .protocolVersion(mqttVersion)
                .username(username)
                .password(password.getBytes(CharsetUtil.UTF_8))
                .properties(properties)
                .willRetain(true)
                .willQoS(MqttQoS.AT_LEAST_ONCE)
                .willFlag(true)
                .willTopic(WILL_TOPIC)
                .willMessage(WILL_MESSAGE.getBytes(CharsetUtil.UTF_8))
                .willProperties(willProperties)
                .cleanSession(true)
                .keepAlive(KEEP_ALIVE_SECONDS)
                .build();
    }

    @Before
    public void setUp() throws IllegalAccessException {
        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SESSION_EXPIRY_INTERVAL.value(), 10));
        props.add(new MqttProperties.StringProperty(AUTHENTICATION_METHOD.value(), "Plain"));
        props.add(new MqttProperties.UserProperty("hello", "1"));
        props.add(new MqttProperties.UserProperty("tag", "secondTag"));
        MqttProperties willProps = new MqttProperties();
        willProps.add(new MqttProperties.IntegerProperty(WILL_DELAY_INTERVAL.value(), 100));

        connectMessage = createConnectMessage(MqttVersion.MQTT_5, USER_NAME, PASSWORD, props, willProps);
        connectHandler = new Mqtt5ConnectHandler();
        FieldUtils.writeDeclaredField(connectHandler, "channelManager", channelManager, true);
        FieldUtils.writeDeclaredField(connectHandler, "sessionLoop", sessionLoop, true);

        when(ctx.channel()).thenReturn(channel);
    }

    @After
    public void After() {
    }

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
            Thread.sleep(1100);
        } catch (InterruptedException ignored) {
        }

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
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        verify(channel).writeAndFlush(any(MqttConnAckMessage.class));
        verify(sessionLoop).loadSession(any(), any());
        verifyNoMoreInteractions(channelManager, sessionLoop);
    }
}
