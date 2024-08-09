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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.CoapRequestType;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapDecoder;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapDecoder {

    private CoapDecoder coapDecoder;
    private DatagramPacket packet;
    private List<Object> out = new ArrayList<>();
    private InetSocketAddress localAddress = new InetSocketAddress("0.0.0.0", 5683);
    private InetSocketAddress remoteAddress = new InetSocketAddress("195.56.3.1", 5683);

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Before
    public void setUp() throws Exception{
        coapDecoder = new CoapDecoder();
        FieldUtils.writeDeclaredField(coapDecoder, "datagramChannelManager", datagramChannelManager, true);
    }

    @Test
    public void testInvalidShortHeader() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_REQUEST);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testInvalidCoapVersion() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0xC0); // Version=3, Type=0, TokenLength=0
        in.writeByte(0x01); // Code=GET
        in.writeShort(0x0101);  // Message ID=257
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_REQUEST);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testInvalidLongToken() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x49); // Version=1, Type=0, TokenLength=9
        in.writeByte(0x01); // Code=GET
        in.writeShort(0x0101);  // Message ID=257
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_REQUEST);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testInvalidCoapCode() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x05); // Invalid Code=5
        in.writeShort(0x0101);  // Message ID=257
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_REQUEST);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testInvalidToken() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x43); // Version=1, Type=0, TokenLength=3
        in.writeByte(0x01); // Code=GET
        in.writeShort(0x0101);  // Message ID=257
        in.writeByte(0x01); // Invalid token, shorter then tokenLength
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_REQUEST);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidAck() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x60); // Version=1, Type=2, TokenLength=0
        in.writeByte(0x00); // Code=ACK
        in.writeShort(0x0101);  // Message ID=257
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(ctx).fireChannelRead(argThat(msg -> {
            assertEquals(((CoapRequestMessage) msg).getRequestType(), CoapRequestType.ACK);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testInvalidOptionNumber() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x01); // Code=GET
        in.writeShort(0x0101);  // Message ID=257
        in.writeByte(0x20); // Invalid Option Number=2
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.BAD_OPTION);
            return true;
        }));
        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidSubscribe() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x01); // Code=GET
        in.writeShort(0x0101);  // Message ID=257
        in.writeByte(0x60); // Option: Observe, 0
        // Construct Option URI-PATH: ps/topic1/r1
        in.writeByte(0x52); // Option: URI-Path, option value length=2
        in.writeBytes("ps".getBytes());
        in.writeByte(0x06); // Option: URI-Path, option value length=6
        in.writeBytes("topic1".getBytes());
        in.writeByte(0x02); // Option: URI-Path, option value length=6
        in.writeBytes("r1".getBytes());
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.SUBSCRIBE, outMessage.getRequestType());
        assertEquals("topic1/r1", outMessage.getTopic());

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidPublish() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x02); // Code=POST
        in.writeShort(0x0101);  // Message ID=257
        // Construct Option URI-PATH: ps/topic1/r1
        in.writeByte(0xB2); // Option: URI-Path, option value length=2
        in.writeBytes("ps".getBytes());
        in.writeByte(0x06); // Option: URI-Path, option value length=6
        in.writeBytes("topic1".getBytes());
        in.writeByte(0x02); // Option: URI-Path, option value length=6
        in.writeBytes("r1".getBytes());
        in.writeByte(0xFF); // Payload Marker
        in.writeBytes("Hello!".getBytes()); // Payload
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.PUBLISH, outMessage.getRequestType());
        assertEquals("topic1/r1", outMessage.getTopic());
        assertEquals("Hello!", new String(outMessage.getPayload(), StandardCharsets.UTF_8));

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidConnect() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x02); // Code=POST
        in.writeShort(0x0101);  // Message ID=257
        // Construct Option URI-PATH: mqtt/connection
        in.writeByte(0xB4); // Option: URI-Path, option value length=4
        in.writeBytes("mqtt".getBytes());
        in.writeByte(0x0A); // Option: URI-Path, option value length=10
        in.writeBytes("connection".getBytes());
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.CONNECT, outMessage.getRequestType());

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidDisconnect() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x04); // Code=DELETE
        in.writeShort(0x0101);  // Message ID=257
        // Construct Option URI-PATH: mqtt/connection
        in.writeByte(0xB4); // Option: URI-Path, option value length=4
        in.writeBytes("mqtt".getBytes());
        in.writeByte(0x0A); // Option: URI-Path, option value length=10
        in.writeBytes("connection".getBytes());
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.DISCONNECT, outMessage.getRequestType());

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidHeartbeat() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x03); // Code=PUT
        in.writeShort(0x0101);  // Message ID=257
        // Construct Option URI-PATH: mqtt/connection
        in.writeByte(0xB4); // Option: URI-Path, option value length=4
        in.writeBytes("mqtt".getBytes());
        in.writeByte(0x0A); // Option: URI-Path, option value length=10
        in.writeBytes("connection".getBytes());
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.HEARTBEAT, outMessage.getRequestType());

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }

    @Test
    public void testValidQuery() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte(0x40); // Version=1, Type=0, TokenLength=0
        in.writeByte(0x02); // Code=POST
        in.writeShort(0x0101);  // Message ID=257
        // Construct Option URI-PATH: ps/topic1/r1
        in.writeByte(0xB2); // Option: URI-Path, option value length=2
        in.writeBytes("ps".getBytes());
        in.writeByte(0x06); // Option: URI-Path, option value length=6
        in.writeBytes("topic1".getBytes());
        in.writeByte(0x02); // Option: URI-Path, option value length=2
        in.writeBytes("r1".getBytes());
        // Construct Option URI-QUERY: clientid=123, qos=1, retain=true, username=admin, password=public
        in.writeByte(0x4C); // Option: URI-Query, option value length=12
        in.writeBytes("clientid=123".getBytes());
        in.writeByte(0x05);
        in.writeBytes("qos=1".getBytes());
        in.writeByte(0x0B);
        in.writeBytes("retain=true".getBytes());
        in.writeByte(0x0B);
        in.writeBytes("username=01".getBytes());
        in.writeByte(0x0C);
        in.writeBytes("password=111".getBytes());
        in.writeByte(0xFF); // Payload Marker
        in.writeBytes("Hello!".getBytes()); // Payload
        packet = new DatagramPacket(in, localAddress, remoteAddress);

        coapDecoder.decode(ctx, packet, out);

        assertEquals(1, out.size());
        CoapRequestMessage outMessage = (CoapRequestMessage) out.get(0);
        assertEquals(CoapRequestType.PUBLISH, outMessage.getRequestType());
        assertEquals("topic1/r1", outMessage.getTopic());
        assertEquals("123", outMessage.getClientId());
        assertEquals(MqttQoS.AT_LEAST_ONCE, outMessage.getQosLevel());
        assertTrue(outMessage.isReatin());
        assertEquals("01", outMessage.getUserName());
        assertEquals("111", outMessage.getPassword());
        assertEquals("Hello!", new String(outMessage.getPayload(), StandardCharsets.UTF_8));

        verifyNoMoreInteractions(ctx, datagramChannelManager);
    }
}
