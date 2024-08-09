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

import io.netty.channel.socket.DatagramChannel;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapResponseCache;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapRetryManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestDatagramChannelManager {

    private DatagramChannelManager datagramChannelManager;

    @Mock
    private CoapResponseCache coapResponseCache;

    @Mock
    private CoapRetryManager coapRetryManager;

    @Mock
    private DatagramChannel channel;

    @Mock
    private CoapMessage message;

    @Mock
    private CoapSession session;

    @Before
    public void setUp() throws Exception{
        datagramChannelManager = new DatagramChannelManager();
        FieldUtils.writeDeclaredField(datagramChannelManager, "coapResponseCache", coapResponseCache, true);
        FieldUtils.writeDeclaredField(datagramChannelManager, "coapRetryManager", coapRetryManager, true);
        FieldUtils.writeDeclaredField(datagramChannelManager, "channel", channel, true);
    }

    @Test
    public void testWrite() {
        datagramChannelManager.write(message);
        verify(channel).writeAndFlush(message);
        verifyNoMoreInteractions(coapResponseCache, coapRetryManager, channel);
    }

    @Test
    public void testWriteResponse() {
        datagramChannelManager.writeResponse(message);
        verify(channel).writeAndFlush(message);
        verify(coapResponseCache).put(message);
        verifyNoMoreInteractions(coapResponseCache, coapRetryManager, channel);
    }

    @Test
    public void testPushMessage() {
        when(message.getType()).thenReturn(CoapMessageType.CON);

        datagramChannelManager.pushMessage(session, message);
        verify(channel).writeAndFlush(message);
        verify(coapRetryManager).addRetryMessage(session, message);
        verifyNoMoreInteractions(coapResponseCache, coapRetryManager, channel);
    }


}
