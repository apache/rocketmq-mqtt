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
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.channel.ConnectHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

@RunWith(MockitoJUnitRunner.class)
public class TestConnectHandler {

    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Logger logger;
    @Mock
    private Channel channel;
    @Mock
    private ChannelManager channelManager;

    @Test
    public void test() throws Exception {
        ConnectHandler connectHandler=new ConnectHandler();
        Throwable throwable=new Exception("Connection reset by peer");
        when(ctx.channel()).thenReturn(channel);
        FieldUtils.writeDeclaredField(connectHandler,"channelManager",channelManager,true);
        FieldUtils.writeDeclaredField(connectHandler,"logger",logger,true);
        connectHandler.exceptionCaught(ctx,throwable);
        verify(logger,never()).error(anyString(),any(),any());
        verify(channelManager).closeConnect(channel,ChannelCloseFrom.SERVER, throwable.getMessage());

        connectHandler.exceptionCaught(ctx,new Exception("test exception"));
        verify(logger,only()).error(anyString(),any(),any());
        verify(channelManager).closeConnect(channel,ChannelCloseFrom.SERVER, throwable.getMessage());
    }
}
