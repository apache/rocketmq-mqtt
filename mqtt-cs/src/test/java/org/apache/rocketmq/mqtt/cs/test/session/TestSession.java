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

package org.apache.rocketmq.mqtt.cs.test.session;

import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class TestSession {
    final String topicFilter = "testSession";
    final String queueName = topicFilter;
    final String brokerName = "localhost";
    final int firstMsgOffset = 1;
    final int secondMsgOffset = 2;

    @Test
    public void test() {
        Session session = new Session();

        Set<Subscription> subscriptions = new HashSet<>();
        Subscription subscription = new Subscription(topicFilter);
        subscriptions.add(subscription);
        session.addSubscription(subscriptions);
        Set<Subscription> subscriptionSnapshot = session.subscriptionSnapshot();
        Assert.assertEquals(subscription, subscriptionSnapshot.iterator().next());

        Queue queue = new Queue(0, queueName, brokerName);
        QueueOffset queueOffset = new QueueOffset();
        Map<Queue, QueueOffset> offsetMap = new HashMap<>();
        offsetMap.put(queue, queueOffset);
        Map<String, Map<Queue, QueueOffset>> offsetMapParam = new HashMap<>();
        offsetMapParam.put(queueName, offsetMap);

        session.addOffset(offsetMapParam);
        Assert.assertEquals(queueOffset, session.getQueueOffset(subscription, queue));
        Assert.assertEquals(offsetMap, session.getQueueOffset(subscription));
        Map<Subscription, Map<Queue, QueueOffset>> offsetSnapshot = session.offsetMapSnapshot();
        Assert.assertEquals(1, offsetSnapshot.size());
        Assert.assertEquals(subscription, offsetSnapshot.keySet().iterator().next());
        Assert.assertEquals(offsetMap, offsetSnapshot.values().iterator().next());

        session.freshQueue(subscription, new HashSet<>(Arrays.asList(queue)));
        List<Message> messages = new ArrayList<>();
        Message message = new Message();
        message.setOffset(firstMsgOffset);
        messages.add(message);
        session.addSendingMessages(subscription, queue, messages);
        Assert.assertFalse(session.sendingMessageIsEmpty(subscription, queue));
        Assert.assertEquals(message, session.nextSendMessageByOrder(subscription, queue));
        Assert.assertEquals(message, session.pendMessageList(subscription, queue).iterator().next());

        Message secondMessage = new Message();
        secondMessage.setOffset(secondMsgOffset);
        session.addSendingMessages(subscription, queue, Arrays.asList(secondMessage));
        Assert.assertEquals(2, session.pendMessageList(subscription, queue).size());

        // when ack larger offset, the Msg will not be removed
        session.ack(subscription, queue, secondMsgOffset);
        Assert.assertEquals(Long.MAX_VALUE, session.getQueueOffset(subscription, queue).getOffset());
        Assert.assertEquals(1, session.pendMessageList(subscription, queue).size());
        Assert.assertFalse(session.getPersistOffsetFlag());

        // test rollNext
        Message nextMsg = session.rollNext(subscription, queue, firstMsgOffset);
        Assert.assertTrue(session.getPersistOffsetFlag());
        Assert.assertEquals(secondMsgOffset, session.getQueueOffset(subscription, queue).getOffset());
        Assert.assertEquals(secondMessage, nextMsg);

        // when ack next-offset, the last time acked Msg will be removed
        session.ack(subscription, queue, secondMsgOffset + 1);
        Assert.assertTrue(session.sendingMessageIsEmpty(subscription, queue));

        session.removeSubscription(subscription);
        Assert.assertEquals(0, session.subscriptionSnapshot().size());
        session.destroy();
    }
}
