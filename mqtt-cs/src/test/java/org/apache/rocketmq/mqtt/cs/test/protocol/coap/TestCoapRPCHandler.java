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
package org.apache.rocketmq.mqtt.cs.test.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapRPCHandler;
import org.apache.rocketmq.mqtt.ds.notify.NotifyManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapRPCHandler {

    private CoapRPCHandler coapRPCHandler;

    @Mock
    private MetaPersistManager metaPersistManager;

    @Mock
    private NotifyManager notifyManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DatagramPacket packet;

    @Before
    public void setUp() throws IllegalAccessException {
        coapRPCHandler = new CoapRPCHandler();
        FieldUtils.writeDeclaredField(coapRPCHandler, "metaPersistManager", metaPersistManager, true);
        FieldUtils.writeDeclaredField(coapRPCHandler, "notifyManager", notifyManager, true);

        when(packet.sender()).thenReturn(new InetSocketAddress("125.45.12.1", 5683));
    }

    @Test
    public void testNoConnectNode() throws Exception {
        when(metaPersistManager.getConnectNodeSet()).thenReturn(null);

        assertThrows(RemotingException.class, () -> coapRPCHandler.channelRead(ctx, packet));
        verify(metaPersistManager).getConnectNodeSet();
        verifyNoMoreInteractions(metaPersistManager, notifyManager, ctx);
    }

    @Test
    public void testForLocalhost() throws Exception {
        String localAdress = InetAddress.getLocalHost().getHostAddress();
        Set<String> nodes = new HashSet<>();
        nodes.add(localAdress);
        when(metaPersistManager.getConnectNodeSet()).thenReturn(nodes);

        coapRPCHandler.channelRead(ctx, packet);

        verify(metaPersistManager).getConnectNodeSet();
        verify(ctx).fireChannelRead(packet);
        verifyNoMoreInteractions(metaPersistManager, notifyManager, ctx);
    }

    @Test
    public void testForwardSuccess() throws Exception {
        String forwardAddress = "10.11.123.1";
        Set<String> nodes = new HashSet<>();
        nodes.add(forwardAddress);
        when(metaPersistManager.getConnectNodeSet()).thenReturn(nodes);
        when(notifyManager.doCoapForward(anyString(), any(DatagramPacket.class))).thenReturn(true);

        coapRPCHandler.channelRead(ctx, packet);

        verify(metaPersistManager).getConnectNodeSet();
        verify(notifyManager).doCoapForward(forwardAddress, packet);
        verifyNoMoreInteractions(metaPersistManager, notifyManager, ctx);
    }
}
