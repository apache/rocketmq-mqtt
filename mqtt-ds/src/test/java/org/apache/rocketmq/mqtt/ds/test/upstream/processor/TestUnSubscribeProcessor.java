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

package org.apache.rocketmq.mqtt.ds.test.upstream.processor;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.upstream.processor.UnSubscribeProcessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestUnSubscribeProcessor {

    @Mock
    private FirstTopicManager firstTopicManager;

    @Mock
    private SubscriptionPersistManager subscriptionPersistManager;

    @Test
    public void test() throws IllegalAccessException {
        UnSubscribeProcessor unSubscribeProcessor = new UnSubscribeProcessor();
        FieldUtils.writeDeclaredField(unSubscribeProcessor, "firstTopicManager", firstTopicManager, true);
        FieldUtils.writeDeclaredField(unSubscribeProcessor, "subscriptionPersistManager", subscriptionPersistManager, true);

        MqttMessageUpContext context = new MqttMessageUpContext();
        context.setClientId("test");

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(1);
        MqttUnsubscribePayload payload = new MqttUnsubscribePayload(Arrays.asList("test"));
        MqttUnsubscribeMessage mqttUnsubscribeMessage = new MqttUnsubscribeMessage(mqttFixedHeader, variableHeader, payload);

        unSubscribeProcessor.process(context, mqttUnsubscribeMessage);
        verify(firstTopicManager).checkFirstTopicIfCreated(any());
        verify(subscriptionPersistManager).removeSubscriptions(any(), anySet());
    }

}
