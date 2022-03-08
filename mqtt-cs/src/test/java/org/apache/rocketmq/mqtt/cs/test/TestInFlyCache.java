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
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TestInFlyCache {

    @Test
    public void test() {
        InFlyCache inFlyCache = new InFlyCache();
        inFlyCache.put(InFlyCache.CacheType.PUB, "test", 1);
        Assert.assertTrue(inFlyCache.contains(InFlyCache.CacheType.PUB, "test", 1));

        inFlyCache.getPendingDownCache().put("test", 1, mock(Subscription.class), mock(Queue.class), mock(Message.class));
        Assert.assertTrue(null != inFlyCache.getPendingDownCache().get("test", 1));

        inFlyCache.getPendingDownCache().remove("test", 1);
        Assert.assertTrue(null == inFlyCache.getPendingDownCache().get("test", 1));

    }
}
