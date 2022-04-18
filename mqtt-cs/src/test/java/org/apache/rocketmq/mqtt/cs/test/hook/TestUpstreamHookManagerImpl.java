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

package org.apache.rocketmq.mqtt.cs.test.hook;

import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHook;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.cs.hook.UpstreamHookManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestUpstreamHookManagerImpl {

    private UpstreamHookManager upstreamHookManager;

    @Mock
    private UpstreamHook upstreamHook;

    @Before
    public void Before() {
        upstreamHookManager = new UpstreamHookManagerImpl();
    }

    @After
    public void After() {}

    @Test(expected = IllegalArgumentException.class )
    public void testAddHookIllegalArgException() throws IllegalAccessException {
        FieldUtils.writeDeclaredField(upstreamHookManager, "isAssembled", new AtomicBoolean(true), true);
        upstreamHookManager.addHook(0, upstreamHook);
    }

    @Test
    public void testDoUpstreamHook() {
        upstreamHookManager.addHook(0, upstreamHook);
        upstreamHookManager.doUpstreamHook(mock(MqttMessageUpContext.class), mock(MqttMessage.class));

        verify(upstreamHook, times(1)).getNextHook();
        verify(upstreamHook, times(1)).setNextHook(any());
        verify(upstreamHook, times(1)).doHook(any(), any());
    }
}
