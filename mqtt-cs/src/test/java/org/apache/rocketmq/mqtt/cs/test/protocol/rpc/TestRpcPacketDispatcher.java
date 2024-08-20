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

package org.apache.rocketmq.mqtt.cs.test.protocol.rpc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.RpcCode;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.rpc.RpcPacketDispatcher;
import org.apache.rocketmq.mqtt.cs.session.notify.MessageNotifyAction;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRpcPacketDispatcher {
    private RemotingCommand notifyCommand = RemotingCommand.createResponseCommand(RpcCode.CMD_NOTIFY_MQTT_MESSAGE, null);
    private RemotingCommand closeCommand = RemotingCommand.createResponseCommand(RpcCode.CMD_CLOSE_CHANNEL, null);
    private RemotingCommand notifyCoapCommand = RemotingCommand.createResponseCommand(RpcCode.COM_NOTIFY_COAP_MESSAGE, null);

    private RpcPacketDispatcher packetDispatcher;

    @Mock
    private MessageNotifyAction messageNotifyAction;

    @Mock
    private ChannelManager channelManager;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DatagramChannel datagramChannel;

    @Mock
    private ChannelPipeline pipeline;

    @Mock
    private ChannelHandlerContext coapContext;

    @Before
    public void setUp() throws Exception {
        packetDispatcher = new RpcPacketDispatcher();
        FieldUtils.writeDeclaredField(packetDispatcher, "messageNotifyAction", messageNotifyAction, true);
        FieldUtils.writeDeclaredField(packetDispatcher, "channelManager", channelManager, true);
        FieldUtils.writeDeclaredField(packetDispatcher, "datagramChannelManager", datagramChannelManager, true);
    }

    @Test
    public void testProcessRequestNotify() throws Exception {
        notifyCommand.setBody(JSON.toJSONBytes(new HashSet<>()));
        packetDispatcher.processRequest(ctx, notifyCommand);

        verify(messageNotifyAction).notify(anyList());
        verifyNoMoreInteractions(messageNotifyAction, channelManager, ctx);
    }

    @Test
    public void testProcessRequestClose() throws Exception {
        closeCommand.setBody(JSON.toJSONBytes(new HashSet<>()));
        packetDispatcher.processRequest(ctx, closeCommand);

        verify(channelManager).closeConnect(any(), any());
        verifyNoMoreInteractions(messageNotifyAction, channelManager, ctx);
    }

    @Test
    public void testProcessRequestFail() throws Exception {
        // set notifyCommand body null to throw NPE exception
        RemotingCommand response = packetDispatcher.processRequest(ctx, notifyCommand);
        Assert.assertEquals(RpcCode.FAIL, response.getCode());
        verifyNoMoreInteractions(messageNotifyAction, channelManager, ctx);
    }

    @Test
    public void testRejectRequest() {
        Assert.assertFalse(packetDispatcher.rejectRequest());
    }

    @Test
    public void testProcessRequestNotifyCoap() throws Exception {
        when(datagramChannelManager.getChannel()).thenReturn(datagramChannel);
        when(datagramChannel.pipeline()).thenReturn(pipeline);
        when(pipeline.context("coap-handler")).thenReturn(coapContext);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", new byte[]{1, 2, 3});
        jsonObject.put("senderAddress", "127.0.0.1");
        jsonObject.put("senderPort", 1234);
        jsonObject.put("recipientAddress", "192.168.1.1");
        jsonObject.put("recipientPort", 5678);

        notifyCoapCommand.setBody(jsonObject.toJSONString().getBytes(StandardCharsets.UTF_8));
        packetDispatcher.processRequest(ctx, notifyCoapCommand);

        verify(datagramChannelManager).getChannel();
        verify(datagramChannel).pipeline();
        verify(pipeline).context("coap-handler");
        verify(coapContext).fireChannelRead(any(DatagramPacket.class));
        verifyNoMoreInteractions(messageNotifyAction, channelManager, ctx);
    }
}
