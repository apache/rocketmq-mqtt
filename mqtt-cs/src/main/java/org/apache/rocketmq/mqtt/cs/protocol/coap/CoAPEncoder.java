package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.DatagramPacketDecoder;
import io.netty.handler.codec.DatagramPacketEncoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.net.InetSocketAddress;


public class CoAPEncoder extends MessageToByteEncoder<CoAPMessage> {
    @Override
    public void encode(ChannelHandlerContext ctx, CoAPMessage msg, ByteBuf out) throws Exception {

        // Handle Version | Type | TokenLength
        byte firstByte = (byte)((msg.getVersion() << 6) | (msg.getType() << 4) | (msg.getTokenLength() & 0x0F));
        out.writeByte(firstByte);

        // Handle Code, MessageID, Token
        out.writeByte(msg.getCode());
        out.writeShort(msg.getMessageId());
        out.writeBytes(msg.getToken());

        // Handle Options
        int prevOptionNumber = 0;
        for (CoAPOption option : msg.getOptions()) {
            int optionDelta = option.getOptionNumber() - prevOptionNumber;
            prevOptionNumber = option.getOptionNumber();
            int optionLength = option.getOptionValue().length;

            if (optionDelta < 13) {
                out.writeByte((byte)((optionDelta << 4) | (optionLength & 0x0F)));
            } else if (optionDelta < 269) {
                out.writeByte((byte)((13 << 4) | (optionLength & 0x0F)));
                out.writeByte(optionDelta - 13);
            } else {
                out.writeByte((byte)((14 << 4) | (optionLength & 0x0F)));
                out.writeShort(optionDelta - 269);
            }

            if (optionLength > 12 && optionLength < 269) {
                out.writeByte(optionLength - 13);
            } else if (optionLength >= 269) {
                out.writeShort(optionLength - 269);
            }

            out.writeBytes(option.getOptionValue());
        }

        // Handle Payload if not empty
        if (msg.getPayload() != null && msg.getPayload().length > 0) {
            out.writeByte((byte)0xFF);
            out.writeBytes(msg.getPayload());
        }

        // Send Response
//        DatagramPacket responsePacket = new DatagramPacket(out, ctx.channel().remoteAddress());
//        ctx.writeAndFlush(responsePacket);

    }

}
