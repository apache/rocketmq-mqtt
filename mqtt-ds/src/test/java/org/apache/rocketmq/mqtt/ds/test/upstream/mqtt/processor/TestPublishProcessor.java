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

package org.apache.rocketmq.mqtt.ds.test.upstream.mqtt.processor;

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
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.PublishProcessor;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPublishProcessor {

    @Mock
    private LmqQueueStore lmqQueueStore;

    @Mock
    private WildcardManager wildcardManager;

    @Mock
    private FirstTopicManager firstTopicManager;

    @Test
    public void test() throws IllegalAccessException, ExecutionException, InterruptedException, RemotingException, com.alipay.sofa.jraft.error.RemotingException {
        PublishProcessor publishProcessor = new PublishProcessor();
        FieldUtils.writeDeclaredField(publishProcessor, "lmqQueueStore", lmqQueueStore, true);
        FieldUtils.writeDeclaredField(publishProcessor, "wildcardManager", wildcardManager, true);
        FieldUtils.writeDeclaredField(publishProcessor, "firstTopicManager", firstTopicManager, true);

        MqttMessageUpContext upContext = new MqttMessageUpContext();
        upContext.setNamespace("testPubProcessor");

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("test", 0);
        ByteBuf payload = Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8));
        MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);

        Set<String> queues = Collections.singleton("testQueue");
        when(wildcardManager.matchQueueSetByMsgTopic(anyString(), anyString())).thenReturn(queues);
        CompletableFuture<StoreResult> storeResultFuture = new CompletableFuture<>();
        StoreResult storeResult = new StoreResult();
        storeResultFuture.complete(storeResult);
        when(lmqQueueStore.putMessage(anySet(), any(Message.class))).thenReturn(storeResultFuture);

        CompletableFuture<HookResult> hookResultCompletableFuture = publishProcessor.process(upContext, publishMessage);

        verify(firstTopicManager).checkFirstTopicIfCreated(anyString());
        Assert.assertNull(hookResultCompletableFuture.get().getRemark());
        Assert.assertNotNull(hookResultCompletableFuture.get().getData());
    }

}
