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
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPingHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqttPingHandler {

    private MqttPingHandler pingHandler;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private MqttMessage mqttMessage;

    @Mock
    private HookResult hookResult;

    @Spy
    private NioSocketChannel channel;

    @Test
    public void testDoHandler() {
        pingHandler = new MqttPingHandler();
        when(ctx.channel()).thenReturn(channel);
        doReturn(null).when(channel).writeAndFlush(any());

        pingHandler.doHandler(ctx, mqttMessage, hookResult);
        verify(ctx).channel();
        verify(channel).writeAndFlush(any());
    }
}
