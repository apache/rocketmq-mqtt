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

package org.apache.rocketmq.mqtt.cs.test.session.infly;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRetryDriver {

    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private PushAction pushAction;

    @Mock
    private ConnectConf connectConf;

    @Mock
    private LmqQueueStore lmqQueueStore;

    @Mock
    private Session session;

    @Mock
    private Message message;

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private Queue queue;

    @Mock
    private QueueFresh queueFresh;

    private RetryDriver retryDriver = new RetryDriver();
    private String channelId = "testRetryDriver";
    private String topicFilter = "/test/retryDriver";
    private Subscription subscription = new Subscription(topicFilter);
    private int messageId = 1;
    private int pubRelMsgId = 2;
    private int scheduleDelaySecs = 1;
    private long messageRetryInterval = 200;
    private int retryIntervalSecs = scheduleDelaySecs;
    private int maxRetryTime = 1;
    private CompletableFuture<StoreResult> completableFuture = new CompletableFuture<>();

    @Before
    public void before() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(retryDriver, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(retryDriver, "pushAction", pushAction, true);
        FieldUtils.writeDeclaredField(retryDriver, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(retryDriver, "lmqQueueStore", lmqQueueStore, true);
        FieldUtils.writeDeclaredField(retryDriver, "scheduleDelaySecs", scheduleDelaySecs, true);
        FieldUtils.writeDeclaredField(retryDriver, "inFlyCache", inFlyCache, true);
        FieldUtils.writeDeclaredField(retryDriver, "queueFresh", queueFresh, true);
        FieldUtils.writeDeclaredField(retryDriver, "messageRetryInterval", messageRetryInterval, true);

        when(connectConf.getRetryIntervalSeconds()).thenReturn(retryIntervalSecs);
        when(connectConf.getMaxRetryTime()).thenReturn(maxRetryTime);
        retryDriver.init();

        when(session.getChannelId()).thenReturn(channelId);
        when(session.isDestroyed()).thenReturn(false);
        when(sessionLoop.getSession(any())).thenReturn(session);
        when(sessionLoop.getSessionList(any())).thenReturn(Collections.singletonList(session));
        when(message.copy()).thenReturn(message);

        StoreResult storeResult = new StoreResult();
        storeResult.setQueue(queue);
        storeResult.setMsgId(String.valueOf(messageId));
        completableFuture.complete(storeResult);
        when(lmqQueueStore.putMessage(any(), any())).thenReturn(completableFuture);
    }

    @Test
    public void testPublish() throws Exception {
        when(connectConf.getSizeOfNotRollWhenAckSlow()).thenReturn(10);
        InFlyCache.PendingDownCache cache = new InFlyCache().getPendingDownCache();
        cache.put(channelId, messageId, subscription, queue, message);
        when(inFlyCache.getPendingDownCache()).thenReturn(cache);

        retryDriver.mountPublish(messageId, message, 1, channelId, subscription);
        Assert.assertTrue(retryDriver.needRetryBefore(subscription, queue, session));

        // sleep 1200 to trigger 'doRetryCache' and executing only once
        // meanwhile set 'messageRetryInterval = 200' to skip continue judgment
        Thread.sleep(1200);

        verify(pushAction, atLeastOnce()).write(any(), any(), eq(messageId), eq(1), any());
        Assert.assertNotNull(retryDriver.unMountPublish(messageId, channelId));
    }

    @Test
    public void testPubRel() throws InterruptedException {
        retryDriver.mountPubRel(pubRelMsgId, channelId);

        // sleep 1200 to trigger 'doRetryCache' and executing only once
        // meanwhile set 'messageRetryInterval = 200' to skip continue judgment
        Thread.sleep(1200);

        verify(sessionLoop).getSession(eq(channelId));
        Assert.assertNotNull(retryDriver.unMountPubRel(pubRelMsgId, channelId));
    }

    @Test
    public void testUnloadSession() throws InterruptedException {
        when(connectConf.getSizeOfNotRollWhenAckSlow()).thenReturn(10);
        when(queueFresh.freshQueue(any(), any())).thenReturn(Collections.singleton(queue));

        retryDriver.mountPublish(messageId, message, 1, channelId, subscription);
        retryDriver.unloadSession(session);

        // wait to execute the scheduled runnable of 'saveRetryQueue'
        Thread.sleep(1500);

        verify(lmqQueueStore).putMessage(any(), any());
        verify(pushAction).rollNext(eq(session), eq(messageId));
        verify(sessionLoop).getSessionList(any());
        verify(sessionLoop).notifyPullMessage(any(), any(), any());
        Assert.assertNull(retryDriver.unMountPublish(messageId, channelId));
    }

}
