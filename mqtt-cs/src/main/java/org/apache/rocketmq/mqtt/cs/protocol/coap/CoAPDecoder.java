package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.rocketmq.mqtt.cs.config.CoAPConf;

import java.util.ArrayList;
import java.util.List;

public class CoAPDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> list) throws Exception {

        // The length of CoAP message is at least 4 bytes.
        if (in.readableBytes() < 4) {
            // TODO: Handle error
            return;
        }

        // Handle first byte, including version, type, and token length.
        int firstByte = in.readUnsignedByte();
        int version = (firstByte >> 6) & 0x03;
        if (version != CoAPConf.VERSION) {
            // TODO: Handle error
            return;
        }
        int type = (firstByte >> 4) & 0x03;
        // TODO: 映射到TYPE，且处理范围外的值
        int tokenLength = firstByte & 0x0F;
        if (tokenLength > CoAPConf.MAX_TOKEN_LENGTH) {
            // TODO: Handle error
            return;
        }

        // Handle second byte, code
        int code = in.readUnsignedByte();
        // TODO: 映射到CODE，且处理范围外的值
        // Handle messageID
        int messageId = in.readUnsignedShort();

        // Handle Token
        if (in.readableBytes() < tokenLength) {
            // TODO: Handle error
            return;
        }
        byte[] token = new byte[tokenLength];
        in.readBytes(token);    // TODO: 这里再确认一下readBytes会不会出问题

        // Handle options
        int nextByte = 0;
        int optionNumber = 0;
        List<CoAPOption> options = new ArrayList<CoAPOption>();
        while (in.readableBytes() > 0) {

            nextByte = in.readUnsignedByte();
            if (nextByte == CoAPConf.PAYLOAD_MARKER) {
                break;
            }

            int optionDelta = nextByte >> 4;
            int optionLength = nextByte & 0x0F;

            if (optionDelta == 13) {
                optionDelta += in.readUnsignedByte();
            } else if (optionDelta == 14) {
                optionDelta += 255 + in.readUnsignedShort();
            } else if (optionDelta == 15) {
                // TODO: Handle error
                return;
            }

            optionNumber += optionDelta;    // current optionNumber = last optionNumber + optionDelta

            if (optionLength == 13) {
                optionLength += in.readUnsignedByte();
            } else if (optionLength == 14) {
                optionLength += 255 + in.readUnsignedShort();
            } else if (optionLength == 15) {
                // TODO: Handle error
                return;
            }

            if (in.readableBytes() < optionLength) {
                // TODO: Handle error
                return;
            }
            byte[] optionValue = new byte[optionLength];
            in.readBytes(optionValue);  // TODO: 这里再确认一下readBytes会不会出问题

            list.add(new CoAPOption(optionNumber, optionValue));
        }

        // Handle payload
        byte[] payload = null;
        if (in.readableBytes() > 0) {
            payload = new byte[in.readableBytes()];
            in.readBytes(payload);
        }

        CoAPMessage coAPMessage = new CoAPMessage(version, type, tokenLength, code, messageId, token, options, payload);

    }
}
