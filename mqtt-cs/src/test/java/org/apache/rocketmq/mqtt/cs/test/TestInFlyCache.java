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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestInFlyCache {
    private final String clientId = "testInFlyCache";
    private final String channelId = "testInFlyCache";
    private final int msgId = 1;

    private InFlyCache inFlyCache;

    @Mock
    private Subscription subscription;

    @Mock
    private Queue queue;

    @Mock
    private Message message;

    @Spy
    private MqttMsgId mqttMsgId;

    @Before
    public void setUp() throws Exception {
        inFlyCache = new InFlyCache();
        mqttMsgId.init();
        FieldUtils.writeDeclaredField(inFlyCache, "mqttMsgId", mqttMsgId, true);
    }

    @Test
    public void test() {
        // cache put
        inFlyCache.put(InFlyCache.CacheType.PUB, channelId, msgId);
        Assert.assertTrue(inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, msgId));
        InFlyCache.PendingDownCache pendingDownCache = inFlyCache.getPendingDownCache();
        pendingDownCache.put(channelId, msgId, subscription, queue, message);
        assertEquals(subscription, pendingDownCache.get(channelId, msgId).getSubscription());
        assertEquals(queue, pendingDownCache.get(channelId, msgId).getQueue());
        assertEquals(1, pendingDownCache.all(channelId).size());

        // cache remove
        pendingDownCache.remove(channelId, msgId);
        Assert.assertNull(pendingDownCache.get(channelId, msgId));
        inFlyCache.remove(InFlyCache.CacheType.PUB, channelId, msgId);
        Assert.assertFalse(inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, msgId));

        // test cleanResource
        inFlyCache.put(InFlyCache.CacheType.PUB, channelId, msgId);
        pendingDownCache.put(channelId, msgId, subscription, queue, message);
        inFlyCache.cleanResource(clientId, channelId);
        Assert.assertNull(pendingDownCache.get(channelId, msgId));
        Assert.assertFalse(inFlyCache.contains(InFlyCache.CacheType.PUB, channelId, msgId));
    }

    @Test (expected = RuntimeException.class)
    public void testInvalidCacheType() {
        inFlyCache.put(InFlyCache.CacheType.valueOf("SUB"), channelId, msgId);
    }
}
