package org.apache.rocketmq.mqtt.cs.test.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPEncoder;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPMessage;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoAPOption;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestCoAPEncoder {

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @InjectMocks
    private CoAPEncoder coAPEncoder;

    private CoAPMessage msg;

    @Before
    public void setUp() {
        CoAPOption option = new CoAPOption(5, new byte[]{5, 6, 7, 8});
        List<CoAPOption> options = new ArrayList<>();
        InetSocketAddress remoteAddress = new InetSocketAddress("localhost", 1234);
        options.add(option);
        msg = new CoAPMessage(1, 0, 4, 64, 1234, new byte[]{1, 2, 3, 4}, options, new byte[]{0, 1}, remoteAddress);
    }

    @Test
    public void testEncode() throws Exception {
        ByteBuf out = Unpooled.buffer();
        coAPEncoder.encode(channelHandlerContext, msg, out);
        byte[] result = new byte[out.readableBytes()];
        out.readBytes(result);

        byte[] expected = {(byte)0x44, (byte)0x40, (byte)0x04, (byte)0xD2, 1, 2, 3, 4, (byte)0x54, 5, 6, 7, 8, (byte)0xFF, 0, 1};
        assertArrayEquals("Encoding mismatch", expected, result);
    }

    @Test
    public void testEncodeWithNoPayload() throws Exception {
        msg.setPayload(new byte[0]); // Setting payload to empty

        ByteBuf out = Unpooled.buffer();
        coAPEncoder.encode(channelHandlerContext, msg, out);
        byte[] result = new byte[out.readableBytes()];
        out.readBytes(result);

        byte[] expected = {(byte)0x44, (byte)0x40, (byte)0x04, (byte)0xD2, 1, 2, 3, 4, (byte)0x54, 5, 6, 7, 8};
        assertArrayEquals("Encoding with no payload mismatch", expected, result);
    }

    @Test
    public void testEncodeWithNoOptionsAndNoPayload() throws Exception {
        msg.setOptions(new ArrayList<>()); // Setting options to empty
        msg.setPayload(new byte[0]); // Setting payload to empty

        ByteBuf out = Unpooled.buffer();
        coAPEncoder.encode(channelHandlerContext, msg, out);
        byte[] result = new byte[out.readableBytes()];
        out.readBytes(result);

        byte[] expected = {(byte)0x44, (byte)0x40, (byte)0x04, (byte)0xD2, 1, 2, 3, 4};
        assertArrayEquals("Encoding with no options and no payload mismatch", expected, result);
    }

}
