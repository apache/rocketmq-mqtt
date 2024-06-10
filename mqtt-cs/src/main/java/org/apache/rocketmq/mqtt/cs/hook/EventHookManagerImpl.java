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

package org.apache.rocketmq.mqtt.cs.hook;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.hook.EventHook;
import org.apache.rocketmq.mqtt.common.hook.EventHookManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.ClientEvent;
import org.apache.rocketmq.mqtt.common.model.EventType;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.mqtt.common.model.Constants.CLIENT_EVENT_BATCH_SIZE;
import static org.apache.rocketmq.mqtt.common.model.Constants.CLIENT_EVENT_ORIGIN_TOPIC;

@Component
public class EventHookManagerImpl implements EventHookManager {
    @Resource
    private MqttMsgId mqttMsgId;
    private static Logger logger = LoggerFactory.getLogger(EventHookManagerImpl.class);
    private EventHook eventHook;
    private AtomicBoolean isAssembled = new AtomicBoolean(false);
    private LinkedBlockingQueue<ClientEvent> eventQueue = new LinkedBlockingQueue<>(10000);
    private ExecutorService executorService;

    @PostConstruct
    public void init() {
        executorService = new ThreadPoolExecutor(
                1,
                1,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(5000),
                new ThreadFactoryImpl("EventHookManager_"));
        executorService.submit(new ClientEventHookExecution());
    }

    @Override
    public void addHook(EventHook eventHook) {
        if (isAssembled.get()) {
            throw new IllegalArgumentException("EventHook Was Assembled");
        }
        this.eventHook = eventHook;
        isAssembled.set(true);
    }

    private class ClientEventHookExecution implements Runnable {
        @Override
        public void run() {
            while (true) {
                int eventBatchSize = CLIENT_EVENT_BATCH_SIZE;
                List<ClientEvent> clientEvents = new ArrayList<>(eventBatchSize);

                try {
                    // batch online offline events
                    while (eventBatchSize-- > 0) {
                        ClientEvent clientEvent = eventQueue.poll(10, TimeUnit.MILLISECONDS);
                        if (clientEvent != null) {
                            clientEvents.add(clientEvent);
                        }
                    }
                    if (clientEvents.isEmpty()) {
                        continue;
                    }

                    // covert to mqtt message
                    List<MqttPublishMessage> eventPublishMessages = toMqttMessage(clientEvents);
                    CompletableFuture<HookResult> eventHookResult = eventHook.doHook(eventPublishMessages);
                    Preconditions.checkNotNull(eventHookResult, "Put client events to LMQ error by null hook result.");

                    eventHookResult.whenComplete((hookResult, throwable) -> {
                        if (throwable != null) {
                            logger.error("Put client events to LMQ error:", throwable);
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("Put client events to LMQ success: {}", hookResult);
                        }
                    });
                } catch (Throwable t) {
                    logger.error("ClientEventHookExecution error: ", t);
                } finally {
                    // release msgId
                    releaseMsgId(clientEvents);
                }
            }
        }
    }

    public List<MqttPublishMessage> toMqttMessage(List<ClientEvent> clientEvents) {
        List<MqttPublishMessage> eventPublishMessages = new ArrayList<>(clientEvents.size());

        for (ClientEvent clientEvent : clientEvents) {
            MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH,
                    false, MqttQoS.AT_LEAST_ONCE, false, 0);
            MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(
                    CLIENT_EVENT_ORIGIN_TOPIC, clientEvent.getPacketId());
            ByteBuf payload = Unpooled.wrappedBuffer(clientEvent.toString().getBytes(StandardCharsets.UTF_8));
            MqttPublishMessage eventPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, payload);

            eventPublishMessages.add(eventPublishMessage);
        }
        return eventPublishMessages;
    }

    private void releaseMsgId(List<ClientEvent> clientEvents) {
        for (ClientEvent clientEvent : clientEvents) {
            mqttMsgId.releaseId(clientEvent.getPacketId(), clientEvent.getClientId());
        }
    }

    @Override
    public void putEvent(Channel channel, EventType eventType, String eventInfo) {
        ClientEvent clientEvent = new ClientEvent(eventType);
        clientEvent.setChannelId(ChannelInfo.getId(channel))
                .setClientId(ChannelInfo.getClientId(channel))
                .setPacketId(mqttMsgId.nextId(ChannelInfo.getClientId(channel)))
                .setEventInfo(eventInfo);

        if (channel.remoteAddress() instanceof InetSocketAddress) {
            InetSocketAddress clientAddress = (InetSocketAddress) channel.remoteAddress();
            clientEvent.setHost(clientAddress.getHostName())
                    .setIp(clientAddress.getAddress().getHostAddress())
                    .setPort(clientAddress.getPort());
        }

        if (!eventQueue.offer(clientEvent)) {
            logger.error("EventQueue is full, putEvent failed: {}", clientEvent);
        }
    }
}
