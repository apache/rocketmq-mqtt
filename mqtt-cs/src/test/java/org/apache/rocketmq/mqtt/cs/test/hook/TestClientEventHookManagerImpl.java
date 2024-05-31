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

package org.apache.rocketmq.mqtt.cs.test.hook;

import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.ClientEventHook;
import org.apache.rocketmq.mqtt.common.model.ClientEventMessage;
import org.apache.rocketmq.mqtt.common.model.ClientEventType;
import org.apache.rocketmq.mqtt.cs.hook.ClientEventHookManagerImpl;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.rocketmq.mqtt.common.model.Constants.CLIENT_EVENT_ORIGIN_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestClientEventHookManagerImpl {

    private ClientEventHookManagerImpl clientEventHookManager;
    private final String clientId = "clientId";
    private final String channelId = "channelId";
    private final int packetId = 9641;
    private final String host = "localhost";
    private final int port = 1988;
    private final String ip = "10.0.0.10";

    @Mock
    private ClientEventHook clientEventHook;

    @Mock
    private MqttMsgId mqttMsgId;

    @Spy
    private NioSocketChannel channel;

    @Before
    public void Before() throws IllegalAccessException {
        clientEventHookManager = new ClientEventHookManagerImpl();
        FieldUtils.writeDeclaredField(clientEventHookManager, "clientEventHook", clientEventHook, true);
        FieldUtils.writeDeclaredField(clientEventHookManager, "mqttMsgId", mqttMsgId, true);
    }

    @After
    public void After() {
    }

    @Test
    public void testAddHookIllegalArgException() {
        clientEventHookManager.addHook(clientEventHook);
        assertThrows(IllegalArgumentException.class, () -> clientEventHookManager.addHook(clientEventHook));
    }

    @Test
    public void testToMqttMessage() {
        ClientEventMessage eventMessage = new ClientEventMessage(ClientEventType.CONNECT);
        eventMessage.setChannelId(channelId)
                .setClientId(clientId)
                .setPacketId(packetId)
                .setHost(host)
                .setIp(ip)
                .setPort(port);

        List<ClientEventMessage> eventMessages = new ArrayList<>();
        eventMessages.add(eventMessage);

        List<MqttPublishMessage> publishMessages = clientEventHookManager.toMqttMessage(eventMessages);
        MqttPublishMessage publishMessage = publishMessages.get(0);

        assertEquals(publishMessage.fixedHeader().messageType(), MqttMessageType.PUBLISH);
        assertEquals(publishMessage.variableHeader().packetId(), packetId);
        assertEquals(publishMessage.variableHeader().topicName(), CLIENT_EVENT_ORIGIN_TOPIC);
        assertEquals(publishMessage.payload().toString(StandardCharsets.UTF_8), eventMessage.toString());
    }

    @Test
    public void testPutClientEvent() throws IllegalAccessException {
        clientEventHookManager.putClientEvent(channel, ClientEventType.CONNECT);
        Object eventQueue = FieldUtils.readDeclaredField(clientEventHookManager, "eventQueue", true);
        assertEquals(1, ((LinkedBlockingQueue<ClientEventMessage>) eventQueue).size());
        assertEquals(ClientEventType.CONNECT, ((LinkedBlockingQueue<ClientEventMessage>) eventQueue).poll().getEventType());
    }

    @Test
    public void testClientEventHookExecution() throws InterruptedException {
        clientEventHookManager.init();

        when(clientEventHook.doHook(any())).thenReturn(new CompletableFuture<>());
        doNothing().when(mqttMsgId).releaseId(anyInt(), any());

        clientEventHookManager.putClientEvent(channel, ClientEventType.CONNECT);
        clientEventHookManager.putClientEvent(channel, ClientEventType.DISCONNECT);

        Thread.sleep(2000);

        verify(clientEventHook, times(1)).doHook(any());
        verify(mqttMsgId, times(2)).releaseId(anyInt(), any());
    }
}
