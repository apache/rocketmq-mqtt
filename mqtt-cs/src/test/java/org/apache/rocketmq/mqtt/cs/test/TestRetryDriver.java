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
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

    private RetryDriver retryDriver = new RetryDriver();

    @Before
    public void before() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(retryDriver, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(retryDriver, "pushAction", pushAction, true);
        FieldUtils.writeDeclaredField(retryDriver, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(retryDriver, "lmqQueueStore", lmqQueueStore, true);

        when(connectConf.getRetryIntervalSeconds()).thenReturn(1);
        when(connectConf.getMaxRetryTime()).thenReturn(1);
        retryDriver.init();
    }

    @Test
    public void test() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InterruptedException {
        Session session = mock(Session.class);
        when(session.getChannelId()).thenReturn("test");
        when(session.isDestroyed()).thenReturn(false);
        when(sessionLoop.getSession(any())).thenReturn(session);
        when(lmqQueueStore.putMessage(any(), any())).thenReturn(mock(CompletableFuture.class));
        Message message = mock(Message.class);
        when(message.copy()).thenReturn(mock(Message.class));
        retryDriver.mountPublish(1,message , 1, "test", mock(Subscription.class));
        Thread.sleep(3000);
        MethodUtils.invokeMethod(retryDriver, true, "doRetryCache");
        verify(pushAction, atLeastOnce()).write(any(), any(), eq(1), eq(1), any());
        Thread.sleep(3000);
        MethodUtils.invokeMethod(retryDriver, true, "doRetryCache");
        verify(lmqQueueStore, atLeastOnce()).putMessage(any(), any());
    }

}
