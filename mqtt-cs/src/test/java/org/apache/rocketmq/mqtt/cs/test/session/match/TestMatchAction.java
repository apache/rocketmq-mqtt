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

package org.apache.rocketmq.mqtt.cs.test.session.match;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMatchAction {

    private MatchAction matchAction;
    private Subscription subscription;
    private Set<Subscription> subscriptionSet;
    private Set<Pair<Session, Subscription>> set;

    final String channelId = "testMatchAction";
    final String testTopic = "test";
    final String p2pTopic = "/p2p/test";
    final String retryTopic = "/retry/test";
    final String p2pSecondTopic = "test/p2p/test";

    @Mock
    private Session session;

    @Mock
    private SessionLoop sessionLoop;

    @Before
    public void setUp() throws Exception {
        matchAction = new MatchAction();
        FieldUtils.writeDeclaredField(matchAction, "sessionLoop", sessionLoop, true);

        subscription = new Subscription(testTopic);
        subscriptionSet = new HashSet<>(Collections.singletonList(subscription));
    }

    @Test
    public void testNormalTopic() {
        when(session.getChannelId()).thenReturn(channelId);
        when(sessionLoop.getSession(any())).thenReturn(session);
        when(session.subscriptionSnapshot()).thenReturn(subscriptionSet);

        matchAction.addSubscription(session, subscriptionSet);
        set = matchAction.matchClients(testTopic, "");
        Assert.assertFalse(set.isEmpty());

        matchAction.removeSubscription(session, subscriptionSet);
        set = matchAction.matchClients(testTopic, "");
        Assert.assertTrue(set.isEmpty());
    }

    @Test
    public void testRetryTopic() {
        when(sessionLoop.getSessionList(any())).thenReturn(Collections.singletonList(session));
        set = matchAction.matchClients(retryTopic,"");
        Assert.assertFalse(set.isEmpty());
    }

    @Test
    public void testP2pTopic() {
        when(sessionLoop.getSessionList(any())).thenReturn(Collections.singletonList(session));
        set = matchAction.matchClients(p2pTopic, "");
        Assert.assertFalse(set.isEmpty());
    }

    @Test
    public void testP2pSecondTopic() {
        when(sessionLoop.getSessionList(any())).thenReturn(Collections.singletonList(session));
        set = matchAction.matchClients(p2pSecondTopic, "");
        Assert.assertFalse(set.isEmpty());
    }

}
