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
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPushAction {

    @Mock
    private MqttMsgId mqttMsgId;

    @Mock
    private RetryDriver retryDriver;

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private ConnectConf connectConf;

    private PushAction pushAction = new PushAction();

    @Before
    public void before() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(pushAction, "mqttMsgId", mqttMsgId, true);
        FieldUtils.writeDeclaredField(pushAction, "retryDriver", retryDriver, true);
        FieldUtils.writeDeclaredField(pushAction, "inFlyCache", inFlyCache, true);
        FieldUtils.writeDeclaredField(pushAction, "connectConf", connectConf, true);
    }

    @Test
    public void testMessageArrive() {
        Session session = mock(Session.class);
        Subscription subscription = mock(Subscription.class);
        Queue queue = mock(Queue.class);
        List<Message> messages = new ArrayList<>();
        messages.add(mock(Message.class));
        when(session.pendMessageList(any(), any())).thenReturn(messages);
        when(connectConf.isOrder()).thenReturn(false);
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).push(any(), any(), any(), any());
        spyPushAction.messageArrive(session, subscription, queue);
        verify(spyPushAction, atLeastOnce()).push(any(), any(), any(), any());
    }

    @Test
    public void testPush() {
        Session session = mock(Session.class);
        when(session.getChannelId()).thenReturn("test");
        when(session.getClientId()).thenReturn("test");
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());
        spyPushAction.push(mock(Message.class), mock(Subscription.class), session, mock(Queue.class));
        verify(spyPushAction, atLeastOnce()).write(any(), any(), anyInt(), anyInt(), any());
    }

}
