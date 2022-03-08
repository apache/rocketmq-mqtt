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
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMatchAction {

    @Mock
    private SessionLoop sessionLoop;

    @Test
    public void test() throws IllegalAccessException {
        MatchAction matchAction = new MatchAction();
        FieldUtils.writeDeclaredField(matchAction, "sessionLoop", sessionLoop, true);

        Session session = mock(Session.class);
        when(session.getChannelId()).thenReturn("test");
        when(sessionLoop.getSession(any())).thenReturn(session);
        Subscription subscription = new Subscription("test");
        Set<Subscription> subscriptions = new HashSet<>(Arrays.asList(subscription));
        when(session.subscriptionSnapshot()).thenReturn(subscriptions);

        matchAction.addSubscription(session, subscriptions);
        Set<Pair<Session, Subscription>> set =  matchAction.matchClients("test","");
        Assert.assertFalse(set.isEmpty());

        matchAction.removeSubscription(session,subscriptions);
        set =  matchAction.matchClients("test","");
        Assert.assertTrue(set.isEmpty());
    }

}
