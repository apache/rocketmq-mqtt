package org.apache.rocketmq.mqtt.cs.test.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPDecoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestCoAPDecoder {

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @InjectMocks
    private CoAPDecoder coAPDecoder;

    private List<Object> out;

    @Before
    public void setUp() {
        out = new ArrayList<>();
    }

    @Test
    public void testDecodeCompleteMessage() {
        ByteBuf in = Unpooled.buffer();
        in.writeBytes(new byte[]{(byte)0x44, (byte)0x01, (byte)0x04, (byte)0xD2, 1, 2, 3, 4, (byte)0x54, 5, 6, 7, 8, (byte)0xFF, 0, 1});

        InetSocketAddress senderAddress = new InetSocketAddress("195.0.30.1", 5683);
        InetSocketAddress recipientAddress = new InetSocketAddress("127.0.0.1", 5683);
        DatagramPacket packet = new DatagramPacket(in, recipientAddress, senderAddress);

        try {
            coAPDecoder.decode(channelHandlerContext, packet, out);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(1, out.size());
        assertNotNull(out.get(0));
        assertTrue(out.get(0) instanceof CoAPMessage);

    }

    @Test
    public void testDecodeInsufficientBytes() {
        ByteBuf in = Unpooled.buffer();
        // Less than 4 bytes, which is the minimum length for a CoAP message
        in.writeByte(0x40);
        InetSocketAddress senderAddress = new InetSocketAddress("195.0.30.1", 5683);
        InetSocketAddress recipientAddress = new InetSocketAddress("127.0.0.1", 5683);
        DatagramPacket packet = new DatagramPacket(in, recipientAddress, senderAddress);

        try {
            coAPDecoder.decode(channelHandlerContext, packet, out);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(0, out.size());
        // Verify that the method does not attempt to decode incomplete messages
        verify(channelHandlerContext, times(0)).writeAndFlush(any());
    }

    @Test
    public void testDecodeInvalidVersion() {
        ByteBuf in = Unpooled.buffer();
        // Invalid version field (assuming CoAP version must be 1 as per RFC 7252)
        in.writeBytes(new byte[]{(byte)0x80, 0x00, 0x00, 0x3C});
        InetSocketAddress senderAddress = new InetSocketAddress("195.0.30.1", 5683);
        InetSocketAddress recipientAddress = new InetSocketAddress("127.0.0.1", 5683);
        DatagramPacket packet = new DatagramPacket(in, recipientAddress, senderAddress);

        try {
            coAPDecoder.decode(channelHandlerContext, packet, out);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(0, out.size());
        // Verify that method handles invalid version gracefully
        verify(channelHandlerContext, times(0)).writeAndFlush(any());
    }

}