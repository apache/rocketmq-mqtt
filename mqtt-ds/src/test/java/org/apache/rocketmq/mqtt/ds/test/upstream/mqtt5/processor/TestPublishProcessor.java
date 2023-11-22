/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.ds.test.upstream.mqtt5.processor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.PublishProcessor5;
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

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
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
        PublishProcessor5 publishProcessor5 = new PublishProcessor5();
        FieldUtils.writeDeclaredField(publishProcessor5, "lmqQueueStore", lmqQueueStore, true);
        FieldUtils.writeDeclaredField(publishProcessor5, "wildcardManager", wildcardManager, true);
        FieldUtils.writeDeclaredField(publishProcessor5, "firstTopicManager", firstTopicManager, true);

        MqttMessageUpContext upContext = new MqttMessageUpContext();
        upContext.setNamespace("testPubProcessor");

        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(PAYLOAD_FORMAT_INDICATOR.value(), 6));
        props.add(new MqttProperties.IntegerProperty(PUBLICATION_EXPIRY_INTERVAL.value(), 10));
        props.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), 1));
        props.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), "Response Topic"));
        props.add(new MqttProperties.BinaryProperty(CORRELATION_DATA.value(), "Correlation Data".getBytes(StandardCharsets.UTF_8)));
        props.add(new MqttProperties.StringProperty(CONTENT_TYPE.value(), "Content Type"));

        props.add(new MqttProperties.UserProperty("isSecret", "true"));
        props.add(new MqttProperties.UserProperty("tag", "firstTag"));
        props.add(new MqttProperties.UserProperty("tag", "secondTag"));

        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 100));
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 101));

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("test", 0, props);
        ByteBuf payload = Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8));
        MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);

        Set<String> queues = Collections.singleton("testQueue");
        when(wildcardManager.matchQueueSetByMsgTopic(anyString(), anyString())).thenReturn(queues);
        CompletableFuture<StoreResult> storeResultFuture = new CompletableFuture<>();
        StoreResult storeResult = new StoreResult();
        storeResultFuture.complete(storeResult);
        when(lmqQueueStore.putMessage(anySet(), any())).thenReturn(storeResultFuture);

        CompletableFuture<HookResult> hookResultCompletableFuture = publishProcessor5.process(upContext, publishMessage);

        verify(firstTopicManager).checkFirstTopicIfCreated(anyString());
        Assert.assertNull(hookResultCompletableFuture.get().getRemark());
        Assert.assertNotNull(hookResultCompletableFuture.get().getData());
    }

}
