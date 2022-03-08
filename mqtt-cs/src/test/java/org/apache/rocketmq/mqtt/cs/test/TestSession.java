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

package org.apache.rocketmq.mqtt.cs.test;

import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class TestSession {

    @Test
    public void test() {
        Session session = new Session();

        Set<Subscription> subscriptions = new HashSet<>();
        Subscription subscription = new Subscription("test");
        subscriptions.add(subscription);
        session.addSubscription(subscriptions);
        Set<Subscription> subscriptionSnapshot = session.subscriptionSnapshot();
        Assert.assertTrue(subscriptionSnapshot.iterator().next().equals(subscription));

        Queue queue = new Queue(0, "test", "test");
        QueueOffset queueOffset = new QueueOffset();
        Map<Queue, QueueOffset> offsetMap = new HashMap<>();
        offsetMap.put(queue, queueOffset);
        session.addOffset(subscription.toQueueName(), offsetMap);
        Assert.assertTrue(queueOffset.equals(session.getQueueOffset(subscription, queue)));

        session.freshQueue(subscription, new HashSet<>(Arrays.asList(queue)));
        List<Message> messages = new ArrayList<>();
        Message message = new Message();
        message.setOffset(1);
        messages.add(message);
        session.addSendingMessages(subscription, queue, messages);
        Assert.assertFalse(session.sendingMessageIsEmpty(subscription, queue));
        Assert.assertTrue(message.equals(session.nextSendMessageByOrder(subscription,queue)));
        Assert.assertTrue(message.equals(session.pendMessageList(subscription,queue).iterator().next()));

        session.ack(subscription,queue,1);
        Assert.assertTrue(session.sendingMessageIsEmpty(subscription, queue));
    }

}
