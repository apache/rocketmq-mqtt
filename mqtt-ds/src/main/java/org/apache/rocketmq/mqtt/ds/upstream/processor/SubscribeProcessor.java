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

package org.apache.rocketmq.mqtt.ds.upstream.processor;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.upstream.UpstreamProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class SubscribeProcessor implements UpstreamProcessor {

    @Resource
    private FirstTopicManager firstTopicManager;

    @Override
    public CompletableFuture<HookResult> process(MqttMessageUpContext context, MqttMessage message) {
        MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) message;
        MqttSubscribePayload payload = mqttSubscribeMessage.payload();
        List<MqttTopicSubscription> mqttTopicSubscriptions = payload.topicSubscriptions();
        for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
            String topicFilter = TopicUtils.normalizeTopic(mqttTopicSubscription.topicName());
            MqttTopic mqttTopic = TopicUtils.decode(topicFilter);
            firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic());
        }
        return HookResult.newHookResult(HookResult.SUCCESS, null, null);
    }
}
