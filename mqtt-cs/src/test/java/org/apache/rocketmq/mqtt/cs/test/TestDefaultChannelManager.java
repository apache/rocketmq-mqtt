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

import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Timeout;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.*;

public class TestDefaultChannelManager {

    @Test
    public void test() throws IllegalAccessException, InterruptedException, InvocationTargetException, NoSuchMethodException {
        DefaultChannelManager defaultChannelManager = new DefaultChannelManager();
        SessionLoop sessionLoop = mock(SessionLoop.class);
        FieldUtils.writeDeclaredField(defaultChannelManager, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(defaultChannelManager, "connectConf", mock(ConnectConf.class), true);
        FieldUtils.writeDeclaredField(defaultChannelManager, "retryDriver", mock(RetryDriver.class), true);
        FieldUtils.writeStaticField(DefaultChannelManager.class, "minBlankChannelSeconds", 1, true);
        defaultChannelManager.init();
        NioSocketChannel channel = spy(new NioSocketChannel());
        when(channel.isActive()).thenReturn(false);
        ChannelInfo.setClientId(channel, "test");
        ChannelInfo.setKeepLive(channel, 0);
        defaultChannelManager.addChannel(channel);
        MethodUtils.invokeMethod(defaultChannelManager, true, "doPing", mock(Timeout.class), channel);
        verify(sessionLoop).unloadSession(anyString(), anyString());
    }

}
