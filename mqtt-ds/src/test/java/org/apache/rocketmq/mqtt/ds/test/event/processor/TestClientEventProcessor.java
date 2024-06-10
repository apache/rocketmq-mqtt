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

package org.apache.rocketmq.mqtt.ds.test.event.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.ds.event.processor.ClientEventProcessor;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.rocketmq.mqtt.common.model.Constants.MQTT_SYSTEM_TOPIC;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestClientEventProcessor {

  @Mock
  private LmqQueueStore lmqQueueStore;

  @Mock
  private WildcardManager wildcardManager;

  @Mock
  private FirstTopicManager firstTopicManager;

  @Test
  public void test() throws IllegalAccessException, ExecutionException, InterruptedException, RemotingException, com.alipay.sofa.jraft.error.RemotingException {
    ClientEventProcessor eventProcessor = new ClientEventProcessor();
    FieldUtils.writeDeclaredField(eventProcessor, "lmqQueueStore", lmqQueueStore, true);
    FieldUtils.writeDeclaredField(eventProcessor, "wildcardManager", wildcardManager, true);
    FieldUtils.writeDeclaredField(eventProcessor, "firstTopicManager", firstTopicManager, true);

    MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
    MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("event/online_offline", 169);
    ByteBuf payload = Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8));
    MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
    List<MqttPublishMessage> eventPublishMessages = Collections.singletonList(publishMessage);

    Set<String> queues = Collections.singleton("testQueue");
    when(wildcardManager.matchQueueSetByMsgTopic(anyString(), anyString())).thenReturn(queues);
    CompletableFuture<StoreResult> storeResultFuture = new CompletableFuture<>();
    StoreResult storeResult = new StoreResult();
    storeResultFuture.complete(storeResult);
    when(lmqQueueStore.putMessage(anySet(), anyList())).thenReturn(storeResultFuture);

    CompletableFuture<HookResult> hookResultCompletableFuture = eventProcessor.process(eventPublishMessages);

    verify(firstTopicManager).checkFirstTopicIfCreated(eq(MQTT_SYSTEM_TOPIC));
    verify(wildcardManager).matchQueueSetByMsgTopic(anyString(), anyString());
    verify(lmqQueueStore).putMessage(anySet(), anyList());
    Assert.assertTrue(hookResultCompletableFuture.get().isSuccess());
    Assert.assertNull(hookResultCompletableFuture.get().getRemark());
    Assert.assertNotNull(hookResultCompletableFuture.get().getData());
  }
}
