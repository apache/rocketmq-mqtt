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

package org.apache.rocketmq.mqtt.ds.upstream.processor;

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.SpringUtils;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.upstream.UpstreamProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
public class UnSubscribeProcessor implements UpstreamProcessor {

    @Resource
    private FirstTopicManager firstTopicManager;

    private SubscriptionPersistManager subscriptionPersistManager;

    @Override
    public CompletableFuture<HookResult> process(MqttMessageUpContext context, MqttMessage message) {
        MqttUnsubscribePayload payload = (MqttUnsubscribePayload) message.payload();
        if (payload.topics() != null && !payload.topics().isEmpty()) {
            Set<Subscription> subscriptions = new HashSet<>();
            if (subscriptionPersistManager == null) {
                subscriptionPersistManager = SpringUtils.getBean(SubscriptionPersistManager.class);
            }
            for (String topic : payload.topics()) {
                String topicFilter = TopicUtils.normalizeTopic(topic);
                MqttTopic mqttTopic = TopicUtils.decode(topicFilter);
                firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic());
                Subscription subscription = new Subscription();
                subscription.setTopicFilter(topicFilter);
                subscriptions.add(subscription);
                if (subscriptionPersistManager != null) {
                    subscriptionPersistManager.removeSubscribers(topicFilter, new HashSet<String>() { {
                            add(context.getClientId());
                        } });
                }
            }
            if (subscriptionPersistManager != null) {
                subscriptionPersistManager.removeSubscriptions(context.getClientId(), subscriptions);
            }
        }
        return HookResult.newHookResult(HookResult.SUCCESS, null, null);
    }
}
