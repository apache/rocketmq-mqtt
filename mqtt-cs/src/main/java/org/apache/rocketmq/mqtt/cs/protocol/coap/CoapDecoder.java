package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.rocketmq.mqtt.cs.config.CoapConf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CoapDecoder extends MessageToMessageDecoder<DatagramPacket> {
    @Override
    public void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) throws Exception {

        ByteBuf in = packet.content();

        // The length of Coap message is at least 4 bytes.
        if (in.readableBytes() < 4) {
            // TODO: 包长度过小，返回4.00客户端错误
            return;
        }

        // Handle first byte, including version, type, and token length.
        int firstByte = in.readUnsignedByte();
        int version = (firstByte >> 6) & 0x03;
        if (version != CoapConf.VERSION) {
            // TODO: 版本号不为1，，返回4.00客户端错误
            return;
        }
        int type = (firstByte >> 4) & 0x03;
        int tokenLength = firstByte & 0x0F;
        if (tokenLength > CoapConf.MAX_TOKEN_LENGTH) {
            // TODO: token长度过大，返回4.00客户端错误
            return;
        }

        // Handle code
        int code = in.readUnsignedByte();
        // TODO: 映射到CODE，且处理范围外的值(只接受请求类)
        if (code <= 0 || code >= 5) {
            // TODO: 请求包的code不属于请求类，返回4.00客户端错误
            return;
        }
        // Handle messageID
        int messageId = in.readUnsignedShort();

        // Handle token
        if (in.readableBytes() < tokenLength) {
            // Return 4.00 Response
            CoapMessage response = new CoapMessage(
                    CoapConf.VERSION,
                    type == CoapConf.TYPE.CON.getValue() ? CoapConf.TYPE.ACK.getValue() : CoapConf.TYPE.NON.getValue(),
                    tokenLength,
                    CoapConf.RESPONSE_CODE_CLIENT_ERROR.BAD_REQUEST.getValue(),
                    messageId,
                    null,
                    null,
                    "Format-Error: The length of remaining readable bytes is less than tokenLength!".getBytes(StandardCharsets.UTF_8),
                    packet.sender()
            );
            ctx.writeAndFlush(response);
            return;
        }
        byte[] token = new byte[tokenLength];
        in.readBytes(token);    // TODO: 这里再确认一下readBytes会不会出问题

        // Handle options
        int nextByte = 0;
        int optionNumber = 0;
        List<CoapOption> options = new ArrayList<CoapOption>();
        while (in.readableBytes() > 0) {

            nextByte = in.readUnsignedByte();
            if (nextByte == CoapConf.PAYLOAD_MARKER) {
                break;
            }

            int optionDelta = nextByte >> 4;
            int optionLength = nextByte & 0x0F;

            if (optionDelta == 13) {
                optionDelta += in.readUnsignedByte();
            } else if (optionDelta == 14) {
                optionDelta += 255 + in.readUnsignedShort();
            } else if (optionDelta == 15) {
                // Return 4.00 Response
                CoapMessage response = new CoapMessage(
                        CoapConf.VERSION,
                        type == CoapConf.TYPE.CON.getValue() ? CoapConf.TYPE.ACK.getValue() : CoapConf.TYPE.NON.getValue(),
                        tokenLength,
                        CoapConf.RESPONSE_CODE_CLIENT_ERROR.BAD_REQUEST.getValue(),
                        messageId,
                        token,
                        null,
                        "Format-Error: OptionDelta can not be 15!".getBytes(StandardCharsets.UTF_8),
                        packet.sender()
                );
                ctx.writeAndFlush(response);
                // TODO: 抽象成一个统一的4.00响应包，只是把对应的诊断信息传进去作为Payload
                return;
            }

            optionNumber += optionDelta;    // current optionNumber = last optionNumber + optionDelta

            if (!CoapConf.OPTION_NUMBER.isValid(optionNumber)) {
                // Return 4.02 Response
                CoapMessage response = new CoapMessage(
                        CoapConf.VERSION,
                        type == CoapConf.TYPE.CON.getValue() ? CoapConf.TYPE.ACK.getValue() : CoapConf.TYPE.NON.getValue(),
                        tokenLength,
                        CoapConf.RESPONSE_CODE_CLIENT_ERROR.BAD_OPTION.getValue(),
                        messageId,
                        token,
                        null,
                        "Format-Error: Option number is not defined!".getBytes(StandardCharsets.UTF_8),
                        packet.sender()
                );
                ctx.writeAndFlush(response);
                return;
            }


            if (optionLength == 13) {
                optionLength += in.readUnsignedByte();
            } else if (optionLength == 14) {
                optionLength += 255 + in.readUnsignedShort();
            } else if (optionLength == 15) {
                // Return 4.00 Response
                CoapMessage response = new CoapMessage(
                        CoapConf.VERSION,
                        type == CoapConf.TYPE.CON.getValue() ? CoapConf.TYPE.ACK.getValue() : CoapConf.TYPE.NON.getValue(),
                        tokenLength,
                        CoapConf.RESPONSE_CODE_CLIENT_ERROR.BAD_REQUEST.getValue(),
                        messageId,
                        token,
                        null,
                        "Format-Error: OptionLength can not be 15!".getBytes(StandardCharsets.UTF_8),
                        packet.sender()
                );
                ctx.writeAndFlush(response);
                return;
            }

            if (in.readableBytes() < optionLength) {
                // Return 4.00 Response
                CoapMessage response = new CoapMessage(
                        CoapConf.VERSION,
                        type == CoapConf.TYPE.CON.getValue() ? CoapConf.TYPE.ACK.getValue() : CoapConf.TYPE.NON.getValue(),
                        tokenLength,
                        CoapConf.RESPONSE_CODE_CLIENT_ERROR.BAD_REQUEST.getValue(),
                        messageId,
                        token,
                        null,
                        "Format-Error: The number of readable bytes is less than optionLength".getBytes(StandardCharsets.UTF_8),
                        packet.sender()
                );
                ctx.writeAndFlush(response);
                return;
            }
            byte[] optionValue = new byte[optionLength];
            in.readBytes(optionValue);  // TODO: 这里再确认一下readBytes会不会出问题

            options.add(new CoapOption(optionNumber, optionValue));
        }

        // Handle payload
        byte[] payload = null;
        if (in.readableBytes() > 0) {
            payload = new byte[in.readableBytes()];
            in.readBytes(payload);
        }

        CoapMessage coapMessage = new CoapMessage(version, type, tokenLength, code, messageId, token, options, payload, packet.sender());
        out.add(coapMessage);
    }
}
