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

package org.apache.rocketmq.mqtt.cs.test.channel;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.channel.ConnectHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestConnectHandler {

    private ConnectHandler connectHandler;

    @Mock
    private ChannelManager channelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void Before() throws IllegalAccessException {
        connectHandler = new ConnectHandler();
        FieldUtils.writeDeclaredField(connectHandler, "channelManager", channelManager, true);
    }

    @After
    public void After() {
    }

    @Test
    public void testChannelActive() throws Exception {
        connectHandler.channelActive(ctx);
        verify(channelManager).addChannel(any());
    }

    @Test
    public void testChannelInactive() throws Exception {
        connectHandler.channelInactive(ctx);
        verify(channelManager).closeConnect(any(), any(), any());
    }

    @Test
    public void testExceptionCaught() throws Exception {
        connectHandler.exceptionCaught(ctx, new Throwable("err"));
        verify(channelManager).closeConnect(any(), any(), any());
    }
}
