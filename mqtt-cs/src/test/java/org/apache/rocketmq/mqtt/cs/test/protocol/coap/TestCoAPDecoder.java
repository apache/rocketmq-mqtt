package org.apache.rocketmq.mqtt.cs.test.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPDecoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
        in.writeBytes(new byte[]{0x40, 0x01, 0x00, 0x3C, (byte)0xFF, 0x3E, 0x00, 0x08});


        try {
            coAPDecoder.decode(channelHandlerContext, in, out);
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

        try {
            coAPDecoder.decode(channelHandlerContext, in, out);
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

        try {
            coAPDecoder.decode(channelHandlerContext, in, out);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(0, out.size());
        // Verify that method handles invalid version gracefully
        verify(channelHandlerContext, times(0)).writeAndFlush(any());
    }

}