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

package org.apache.rocketmq.mqtt.cs.test.session.loop;

import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqOffsetStore;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.loop.QueueCache;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoopImpl;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestSessionLoopImpl {

    @Mock
    private MatchAction matchAction;

    @Mock
    private QueueFresh queueFresh;

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private LmqOffsetStore lmqOffsetStore;

    @Mock
    private LmqQueueStore lmqQueueStore;

    @Mock
    private QueueCache queueCache;

    @Mock
    private ConnectConf connectConf;

    private SessionLoopImpl sessionLoop = new SessionLoopImpl();

    @Before
    public void before() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(sessionLoop, "matchAction", matchAction, true);
        FieldUtils.writeDeclaredField(sessionLoop, "queueFresh", queueFresh, true);
        FieldUtils.writeDeclaredField(sessionLoop, "inFlyCache", inFlyCache, true);
        FieldUtils.writeDeclaredField(sessionLoop, "lmqOffsetStore", lmqOffsetStore, true);
        FieldUtils.writeDeclaredField(sessionLoop, "lmqQueueStore", lmqQueueStore, true);
        FieldUtils.writeDeclaredField(sessionLoop, "queueCache", queueCache, true);
        FieldUtils.writeDeclaredField(sessionLoop, "connectConf", connectConf, true);

    }

    @Test
    public void testSessionLoad() throws IllegalAccessException {
        SessionLoopImpl spySessionLoop = spy(sessionLoop);

        NioSocketChannel channel = spy(new NioSocketChannel());
        when(channel.isActive()).thenReturn(true);
        spySessionLoop.loadSession("test", channel);

        Field field = FieldUtils.getField(SessionLoopImpl.class, "sessionMap", true);
        Map<String, Session> sessionMap = (Map<String, Session>) field.get(spySessionLoop);
        Assert.assertFalse(sessionMap.isEmpty());

        List<Session> sessionList = spySessionLoop.getSessionList("test");
        Assert.assertFalse(sessionList.isEmpty());

        spySessionLoop.unloadSession("test", sessionMap.keySet().iterator().next());
        Assert.assertTrue(sessionMap.isEmpty());
    }

    @Test
    public void testAddSubscription() {
        SessionLoopImpl spySessionLoop = spy(sessionLoop);
        Session session = mock(Session.class);
        NioSocketChannel channel = spy(new NioSocketChannel());
        when(session.getChannel()).thenReturn(channel);
        when(spySessionLoop.getSession(anyString())).thenReturn(session);
        QueueOffset queueOffset = new QueueOffset();
        when(session.getQueueOffset(any(), any())).thenReturn(queueOffset);
        Map<Queue, QueueOffset> queueOffsets = new HashMap<>();
        queueOffsets.put(new Queue(), queueOffset);
        when(session.getQueueOffset(any())).thenReturn(queueOffsets);

        CompletableFuture<Long> maxIdResult = new CompletableFuture<>();
        maxIdResult.complete(1L);
        when(lmqQueueStore.queryQueueMaxOffset(any())).thenReturn(maxIdResult);
        spySessionLoop.addSubscription("test", new HashSet<>(Arrays.asList(new Subscription())));
        Assert.assertTrue(queueOffset.isInitialized());
    }

    @Test
    public void testNotifyPullMessage() throws InterruptedException {
        SessionLoopImpl spySessionLoop = spy(sessionLoop);

        Session session = mock(Session.class);
        when(session.getLoadStatusMap()).thenReturn(new ConcurrentHashMap<>());
        QueueOffset queueOffset = new QueueOffset();
        queueOffset.setInitialized();
        when(session.getQueueOffset(any(), any())).thenReturn(queueOffset);
        Map<Queue, QueueOffset> queueOffsets = new HashMap<>();
        Queue queue = new Queue();
        queueOffsets.put(queue, queueOffset);
        when(session.sendingMessageIsEmpty(any(), any())).thenReturn(true);
        NioSocketChannel channel = spy(new NioSocketChannel());
        when(session.getChannel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);

        CompletableFuture<Map<Queue, QueueOffset>> getOffsetResult = new CompletableFuture<>();
        when(lmqOffsetStore.getOffset(any(), any())).thenReturn(getOffsetResult);

        spySessionLoop.init();
        spySessionLoop.notifyPullMessage(session, new Subscription(), queue);

        getOffsetResult.complete(queueOffsets);

        Thread.sleep(1000);

        verify(queueCache, atLeastOnce()).pullMessage(any(), any(), any(), any(), anyInt(), any());

    }

    @Test
    public void testLoadSubscription() throws IllegalAccessException, InterruptedException {
        SubscriptionPersistManager subscriptionPersistManager = mock(SubscriptionPersistManager.class);
        FieldUtils.writeDeclaredField(sessionLoop, "subscriptionPersistManager", subscriptionPersistManager, true);
        SessionLoopImpl spySessionLoop = spy(sessionLoop);
        CompletableFuture<Set<Subscription>> loadResult = new CompletableFuture();
        loadResult.complete(new HashSet<>(Arrays.asList(new Subscription("test"))));
        when(subscriptionPersistManager.loadSubscriptions(any())).thenReturn(loadResult);
        spySessionLoop.init();
        Session session = spy(new Session());
        session.setChannel(new NioSocketChannel());
        when(session.isClean()).thenReturn(Boolean.FALSE);
        spySessionLoop.notifyPullMessage(session, new Subscription("test"), new Queue());
        Assert.assertTrue(session.isLoaded());
    }
}
