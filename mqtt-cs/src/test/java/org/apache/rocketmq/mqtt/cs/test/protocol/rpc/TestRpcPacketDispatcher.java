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
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.RpcCode;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.rpc.RpcPacketDispatcher;
import org.apache.rocketmq.mqtt.cs.session.notify.MessageNotifyAction;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestRpcPacketDispatcher {
    private RemotingCommand notifyCommand = RemotingCommand.createResponseCommand(RpcCode.CMD_NOTIFY_MQTT_MESSAGE, null);
    private RemotingCommand closeCommand = RemotingCommand.createResponseCommand(RpcCode.CMD_CLOSE_CHANNEL, null);

    private RpcPacketDispatcher packetDispatcher;

    @Mock
    private MessageNotifyAction messageNotifyAction;

    @Mock
    private ChannelManager channelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void setUp() throws Exception {
        packetDispatcher = new RpcPacketDispatcher();
        FieldUtils.writeDeclaredField(packetDispatcher, "messageNotifyAction", messageNotifyAction, true);
        FieldUtils.writeDeclaredField(packetDispatcher, "channelManager", channelManager, true);
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
}
