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
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5DisconnectHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class TestMqtt5DisconnectHandler {

    private Mqtt5DisconnectHandler disconnectHandler;

    @Mock
    private ChannelManager channelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private MqttMessage mqttMessage;

    @Mock
    private HookResult hookResult;

    @Spy
    private NioSocketChannel channel;

    @Test
    public void testDoHandlerWithDisConnectOnlyFixedHeader() throws IllegalAccessException {
        disconnectHandler = new Mqtt5DisconnectHandler();
        FieldUtils.writeDeclaredField(disconnectHandler, "channelManager", channelManager, true);

        mqttMessage = MqttMessage.DISCONNECT;

        disconnectHandler.doHandler(ctx, mqttMessage, hookResult);
        verify(channelManager).closeConnect(any(), any(), any());
        verifyNoMoreInteractions(channelManager);
    }

    @Test
    public void testDoHandlerWithDisConnect() throws IllegalAccessException {
        disconnectHandler = new Mqtt5DisconnectHandler();
        FieldUtils.writeDeclaredField(disconnectHandler, "channelManager", channelManager, true);
        when(ctx.channel()).thenReturn(channel);

        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SESSION_EXPIRY_INTERVAL.value(), 6));
        mqttMessage = MqttMessageBuilders.disconnect()
                .reasonCode((byte) 0x96) // Message rate too high
                .properties(props)
                .build();

        disconnectHandler.doHandler(ctx, mqttMessage, hookResult);
        verify(channelManager).closeConnect(any(), any(), any());
        verifyNoMoreInteractions(channelManager);
    }
}