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

package org.apache.rocketmq.mqtt.cs.test.session.notify;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.QueueCache;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.apache.rocketmq.mqtt.cs.session.notify.MessageNotifyAction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMessageNotifyAction {

    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private QueueFresh queueFresh;

    @Mock
    private QueueCache queueCache;

    @Test
    public void test() throws IllegalAccessException {
        MatchAction matchAction = new MatchAction();
        FieldUtils.writeDeclaredField(matchAction, "sessionLoop", sessionLoop, true);

        MessageNotifyAction messageNotifyAction = new MessageNotifyAction();
        FieldUtils.writeDeclaredField(messageNotifyAction, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(messageNotifyAction, "queueFresh", queueFresh, true);
        FieldUtils.writeDeclaredField(messageNotifyAction, "queueCache", queueCache, true);
        FieldUtils.writeDeclaredField(messageNotifyAction, "matchAction", matchAction, true);

        Session session = mock(Session.class);
        when(session.getChannelId()).thenReturn("test");
        when(sessionLoop.getSession(any())).thenReturn(session);
        Subscription subscription = new Subscription("test");
        Set<Subscription> subscriptions = new HashSet<>(Arrays.asList(subscription));
        when(session.subscriptionSnapshot()).thenReturn(subscriptions);
        matchAction.addSubscription(session, subscriptions);

        Queue queue = new Queue(0, "test", "test");
        when(queueFresh.freshQueue(eq(session), eq(subscription))).thenReturn(new HashSet<>(Arrays.asList(queue)));

        MessageEvent messageEvent = new MessageEvent();
        messageEvent.setPubTopic("test");
        messageEvent.setBrokerName("test");
        messageEvent.setQueueId(0);
        messageNotifyAction.notify(Arrays.asList(messageEvent));
        verify(sessionLoop).notifyPullMessage(eq(session), eq(subscription), eq(queue));
    }

}
