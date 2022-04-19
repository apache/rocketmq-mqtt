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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.QueueCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestQueueCache {

    @Mock
    private ConnectConf connectConf;

    @Mock
    private LmqQueueStore lmqQueueStore;

    private QueueCache queueCache = new QueueCache();

    @Before
    public void before() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(queueCache, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(queueCache, "lmqQueueStore", lmqQueueStore, true);
        queueCache.init();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException, TimeoutException {
        when(connectConf.getQueueCacheSize()).thenReturn(32);
        when(connectConf.getPullBatchSize()).thenReturn(32);
        QueueOffset queueOffset = new QueueOffset();
        Queue queue = new Queue();
        queue.setQueueName("test");
        queue.setBrokerName("test");

        List<Message> messageList = new ArrayList<>();
        messageList.add(new Message());
        messageList.add(new Message());
        messageList.get(0).setOffset(1);
        messageList.get(1).setOffset(2);

        CompletableFuture<PullResult> resultPullLast = new CompletableFuture<>();
        PullResult pullResult = new PullResult();
        pullResult.setCode(PullResult.PULL_SUCCESS);
        pullResult.setMessageList(messageList.subList(0, 1));
        resultPullLast.complete(pullResult);
        when(lmqQueueStore.pullLastMessages(any(), any(), anyLong())).thenReturn(resultPullLast);

        CompletableFuture<PullResult> resultPull = new CompletableFuture<>();
        pullResult = new PullResult();
        pullResult.setCode(PullResult.PULL_SUCCESS);
        pullResult.setMessageList(messageList.subList(1, messageList.size()));
        resultPull.complete(pullResult);
        when(lmqQueueStore.pullMessage(any(), any(), any(), anyLong())).thenReturn(resultPull);

        Session session = mock(Session.class);
        queueCache.refreshCache(Pair.of(queue, session));
        Thread.sleep(1000);
        CompletableFuture<PullResult> callBackResult = new CompletableFuture<>();
        queueOffset.setOffset(1);
        queueCache.pullMessage(session, new Subscription("test"), queue, queueOffset, 32, callBackResult);
        pullResult = callBackResult.get(1, TimeUnit.SECONDS);
        Assert.assertTrue(pullResult.getMessageList().get(0).getOffset() == 1);

        queueCache.refreshCache(Pair.of(queue, session));
        Thread.sleep(1000);
        callBackResult = new CompletableFuture<>();
        queueOffset.setOffset(2);
        queueCache.pullMessage(session, new Subscription("test"), queue, queueOffset, 32, callBackResult);
        pullResult = callBackResult.get(1, TimeUnit.SECONDS);
        Assert.assertTrue(pullResult.getMessageList().get(0).getOffset() == 2);
    }

}
