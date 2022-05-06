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

package org.apache.rocketmq.mqtt.cs.test.session;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestQueueFresh {

    private QueueFresh queueFresh;
    private Subscription subscription;
    final String firstTopic = "testQueueFresh";
    final String p2pFilter = "/p2p/test";
    final String retryFilter = "/retry/test";
    final String brokerName = "localhost";
    final Set<String> brokers = Collections.singleton(brokerName);

    @Mock
    private LmqQueueStore lmqQueueStore;

    @Mock
    private Session session;

    @Before
    public void setUp() throws IllegalAccessException {
        queueFresh = new QueueFresh();
        FieldUtils.writeDeclaredField(queueFresh, "lmqQueueStore", lmqQueueStore, true);
        subscription = new Subscription();
    }

    @Test
    public void testP2pQueue() {
        subscription.setTopicFilter(p2pFilter);
        doReturn(null).when(lmqQueueStore).getClientP2pTopic();
        doReturn(null).when(lmqQueueStore).getClientRetryTopic();
        when(lmqQueueStore.getReadableBrokers(any())).thenReturn(brokers);

        Set<Queue> queues = queueFresh.freshQueue(session, subscription);

        verify(lmqQueueStore).getClientP2pTopic();
        verify(lmqQueueStore).getClientRetryTopic();
        verify(lmqQueueStore).getReadableBrokers(any());
        verify(session).freshQueue(eq(subscription), anySet());
        verifyNoMoreInteractions(session, lmqQueueStore);

        Assert.assertEquals(p2pFilter, queues.iterator().next().getQueueName());
        Assert.assertEquals(brokerName, queues.iterator().next().getBrokerName());
    }

    @Test
    public void testRetryQueue() {
        subscription.setTopicFilter(retryFilter);
        doReturn(null).when(lmqQueueStore).getClientRetryTopic();
        when(lmqQueueStore.getReadableBrokers(any())).thenReturn(brokers);

        Set<Queue> queues = queueFresh.freshQueue(session, subscription);

        verify(lmqQueueStore).getClientRetryTopic();
        verify(lmqQueueStore).getReadableBrokers(any());
        verify(session).freshQueue(eq(subscription), anySet());
        verifyNoMoreInteractions(session, lmqQueueStore);

        Assert.assertEquals(retryFilter, queues.iterator().next().getQueueName());
        Assert.assertEquals(brokerName, queues.iterator().next().getBrokerName());
    }

    @Test
    public void testOther() {
        subscription.setTopicFilter(firstTopic);
        doReturn(null).when(lmqQueueStore).getReadableBrokers(firstTopic);

        Set<Queue> queues = queueFresh.freshQueue(session, subscription);

        verify(lmqQueueStore).getReadableBrokers(eq(firstTopic));
        verifyNoMoreInteractions(session, lmqQueueStore);

        Assert.assertTrue(queues.isEmpty());
    }

}
