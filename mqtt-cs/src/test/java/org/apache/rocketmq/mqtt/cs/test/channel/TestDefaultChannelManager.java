/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.cs.test.channel;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.DefaultChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.loop.WillLoop;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;


@RunWith(MockitoJUnitRunner.class)
public class TestDefaultChannelManager {
    private DefaultChannelManager defaultChannelManager;
    private final String clientId = "clientId";
    private final String channelId = "channelId";
    @Mock
    private SessionLoop sessionLoop;

    @Mock
    private ConnectConf connectConf;

    @Mock
    private RetryDriver retryDriver;

    @Mock
    private WillLoop willLoop;

    @Spy
    private NioSocketChannel channel;

    @Before
    public void Before() throws IllegalAccessException {
        defaultChannelManager = new DefaultChannelManager();
        FieldUtils.writeDeclaredField(defaultChannelManager, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(defaultChannelManager, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(defaultChannelManager, "retryDriver", retryDriver, true);
        FieldUtils.writeDeclaredField(defaultChannelManager, "willLoop", willLoop, true);
        FieldUtils.writeStaticField(DefaultChannelManager.class, "minBlankChannelSeconds", 0, true);

        defaultChannelManager.init();
    }

    @After
    public void After() {
        if (channel.isActive()) {
            channel.close();
        }
    }

    @Test
    public void testAddChannel() {
        ChannelInfo.setClientId(channel, clientId);
        ChannelInfo.setChannelLifeCycle(channel, 1000L);
        defaultChannelManager.addChannel(channel);

        // waiting the execution of the 'doPing' TimerTask
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {}

        // verify 'doPing' and 'closeConnect'
        verify(willLoop).closeConnect(eq(channel), eq(clientId), anyString());
        verify(sessionLoop).unloadSession(eq(clientId), anyString());
        verify(retryDriver).unloadSession(Mockito.any());
    }

    @Test
    public void testKeepLive() throws InterruptedException {
        defaultChannelManager.addChannel(channel);
        ChannelInfo.setKeepLive(channel, 1);
        Thread.sleep(200);
        Assert.assertFalse(0 == defaultChannelManager.totalConn());
        Thread.sleep(3000);
        Assert.assertTrue(0 == defaultChannelManager.totalConn());
    }

    @Test
    public void testCloseConnectNullClientId() {
        defaultChannelManager.closeConnect(channel, ChannelCloseFrom.CLIENT, "ForTest");
        verify(sessionLoop).unloadSession(Mockito.isNull(), anyString());
    }

    @Test
    public void testCloseConnect() {
        ChannelInfo.setClientId(channel, clientId);
        defaultChannelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "ForTest");
        verify(willLoop).closeConnect(eq(channel), eq(clientId), anyString());
        verify(sessionLoop).unloadSession(eq(clientId), anyString());
        verify(retryDriver).unloadSession(Mockito.any());
    }

    @Test
    public void testCloseConnectNoExist() throws IllegalAccessException {
        defaultChannelManager.closeConnect(channelId, "ForTest");
        Object channelMap = FieldUtils.readDeclaredField(defaultChannelManager, "channelMap", true);
        Assert.assertEquals(0, ((Map<String, Channel>) channelMap).size());
    }

    @Test
    public void testGetChannelById() {
        Assert.assertNull(defaultChannelManager.getChannelById(channelId));
    }

    @Test
    public void testTotalConn() {
        Assert.assertEquals(0, defaultChannelManager.totalConn());
        defaultChannelManager.addChannel(channel);
        Assert.assertEquals(1, defaultChannelManager.totalConn());
    }

}