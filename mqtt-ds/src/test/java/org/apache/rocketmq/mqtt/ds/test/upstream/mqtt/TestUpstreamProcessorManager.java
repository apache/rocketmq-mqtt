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

package org.apache.rocketmq.mqtt.ds.test.upstream.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.UpstreamProcessorManager;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.PublishProcessor;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestUpstreamProcessorManager {

    @Mock
    private PublishProcessor publishProcessor;

    private UpstreamProcessorManager processorManager;
    private MqttMessageUpContext upContext;
    private MqttPublishVariableHeader variableHeader;
    private ByteBuf payload;

    @Before
    public void SetUp() throws IllegalAccessException {
        processorManager = new UpstreamProcessorManager();
        FieldUtils.writeDeclaredField(processorManager, "publishProcessor", publishProcessor, true);

        upContext = new MqttMessageUpContext();
        variableHeader = new MqttPublishVariableHeader("test", 0);
        payload = Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void test() throws RemotingException, com.alipay.sofa.jraft.error.RemotingException, ExecutionException, InterruptedException {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);

        processorManager.processMqttMessage(upContext, publishMessage);

        verify(publishProcessor).process(eq(upContext), eq(publishMessage));
    }

    @Test
    public void testDefaultCase() throws ExecutionException, InterruptedException, RemotingException, com.alipay.sofa.jraft.error.RemotingException {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);

        CompletableFuture<HookResult> hookResult = processorManager.processMqttMessage(upContext, publishMessage);

        verify(publishProcessor, times(0)).process(any(), any());
        Assert.assertEquals(HookResult.FAIL, hookResult.get().getCode());
    }

}
