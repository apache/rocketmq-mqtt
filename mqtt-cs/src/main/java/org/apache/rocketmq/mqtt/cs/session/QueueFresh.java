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

package org.apache.rocketmq.mqtt.cs.session;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Set;

@Component
public class QueueFresh {

    @Resource
    private LmqQueueStore lmqQueueStore;

    public Set<Queue> freshQueue(Session session, Subscription subscription) {
        Set<Queue> queues = new HashSet<>();
        Set<String> brokers;
        if (subscription.isP2p()) {
            String findTopic = lmqQueueStore.getClientP2pTopic();
            if (StringUtils.isBlank(findTopic)) {
                findTopic = lmqQueueStore.getClientRetryTopic();
            }
            brokers = lmqQueueStore.getReadableBrokers(findTopic);
        } else if (subscription.isRetry()) {
            brokers = lmqQueueStore.getReadableBrokers(lmqQueueStore.getClientRetryTopic());
        } else {
            brokers = lmqQueueStore.getReadableBrokers(subscription.toFirstTopic());
        }
        if (brokers == null || brokers.isEmpty()) {
            return queues;
        }
        for (String broker : brokers) {
            Queue moreQueue = new Queue();
            moreQueue.setQueueName(subscription.toQueueName());
            moreQueue.setBrokerName(broker);
            queues.add(moreQueue);
        }
        session.freshQueue(subscription, queues);
        return queues;
    }

}
