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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapEncoder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapEncoder {

    private CoapEncoder coapEncoder;
    private CoapMessage msg;
    private List<Object> out = new ArrayList<>();

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void setUp() throws Exception{
        coapEncoder = new CoapEncoder();
    }

    @Test
    public void testEncodeAck() throws Exception {
        msg = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.ACK,
                2,
                CoapMessageCode.CREATED,
                1234,
                new byte[]{1,2},
                null,
                new InetSocketAddress("127.0.0.1", 5683)
        );

        coapEncoder.encode(ctx, msg, out);

        assertEquals(1, out.size());
        ByteBuf buffer = ((DatagramPacket) out.get(0)).content();
        assertEquals(0x62, buffer.readByte());  // version=1, type=2, tokenLength=2
        assertEquals(0x41, buffer.readByte()); // code=65
        assertEquals(1234, buffer.readShort()); // messageID
        assertEquals(0x0102, buffer.readShort());   // token

        verifyNoMoreInteractions(ctx);
    }

    @Test
    public void testEncodeNotify() throws Exception {
        msg = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.CON,
                2,
                CoapMessageCode.CONTENT,
                1234,
                new byte[]{1,2},
                "Hello".getBytes(StandardCharsets.UTF_8),
                new InetSocketAddress("127.0.0.1", 5683)
        );

        coapEncoder.encode(ctx, msg, out);

        assertEquals(1, out.size());
        ByteBuf buffer = ((DatagramPacket) out.get(0)).content();
        assertEquals(0x42, buffer.readByte());  // version=1, type=0, tokenLength=2
        assertEquals(0x45, buffer.readByte()); // code=69
        assertEquals(1234, buffer.readShort()); // messageID
        assertEquals(0x0102, buffer.readShort());   // token
        assertEquals(0xFF, buffer.readUnsignedByte());  // payload marker
        assertEquals("Hello", buffer.readCharSequence(5, StandardCharsets.UTF_8).toString());

        verifyNoMoreInteractions(ctx);
    }
}
