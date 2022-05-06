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

package org.apache.rocketmq.mqtt.common.test.model;

import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.junit.Assert;
import org.junit.Test;

public class TestSubscription {

    final String firstTopic = "t1";
    final String topicFilter = firstTopic + "/t2/";
    final int qos = 1;
    final String clientId = "testSub";
    final String p2pTopic = Constants.P2P + clientId + Constants.MQTT_TOPIC_DELIMITER;
    final String retryTopic = Constants.RETRY + clientId + Constants.MQTT_TOPIC_DELIMITER;

    @Test
    public void test() {
        Subscription subscription = new Subscription(topicFilter, qos);
        Assert.assertEquals(firstTopic, subscription.toFirstTopic());
        Assert.assertEquals(topicFilter, subscription.toQueueName());
        Assert.assertEquals(qos, subscription.getQos());

        Assert.assertEquals(p2pTopic, Subscription.newP2pSubscription(clientId).getTopicFilter());
        Assert.assertFalse(new Subscription().isP2p());

        Assert.assertEquals(retryTopic, Subscription.newRetrySubscription(clientId).getTopicFilter());
        Assert.assertTrue(new Subscription(retryTopic).isRetry());
    }
}
