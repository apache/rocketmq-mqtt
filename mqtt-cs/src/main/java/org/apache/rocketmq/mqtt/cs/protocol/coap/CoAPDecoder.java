package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.rocketmq.mqtt.cs.config.CoAPConf;
import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.List;

public class CoAPDecoder extends MessageToMessageDecoder<DatagramPacket> {
    @Override
    public void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) throws Exception {

        ByteBuf in = packet.content();

        // The length of CoAP message is at least 4 bytes.
        if (in.readableBytes() < 4) {
            // TODO: 包长度过小，返回4.00客户端错误
            return;
        }

        // Handle first byte, including version, type, and token length.
        int firstByte = in.readUnsignedByte();
        int version = (firstByte >> 6) & 0x03;
        if (version != CoAPConf.VERSION) {
            // TODO: 版本号不为1，，返回4.00客户端错误
            return;
        }
        int type = (firstByte >> 4) & 0x03;
        // TODO: 映射到TYPE，且处理范围外的值
        if (type < 0 || type > 3) {
            // TODO: type不在规定范围内，返回4.00客户端错误
            return;
        }
        int tokenLength = firstByte & 0x0F;
        if (tokenLength > CoAPConf.MAX_TOKEN_LENGTH) {
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
            // TODO: 剩余字节数少于token长度，返回4.00客户端错误
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
                // TODO: optionDelta不应该为15，返回4.00客户端错误
                CoAPMessage response = new CoAPMessage(
                        CoAPConf.VERSION,
                        CoAPConf.TYPE.RST.getValue(),
                        tokenLength,
                        CoAPConf.RESPONSE_CODE_CLIENT_ERROR.BAD_REQUEST.getValue(),
                        messageId,
                        token,
                        null,
                        null
                );
                ctx.writeAndFlush(response);
                return;
            }

            optionNumber += optionDelta;    // current optionNumber = last optionNumber + optionDelta

            if (optionLength == 13) {
                optionLength += in.readUnsignedByte();
            } else if (optionLength == 14) {
                optionLength += 255 + in.readUnsignedShort();
            } else if (optionLength == 15) {
                // TODO: optionLength不应该为15，返回4.00客户端错误
                return;
            }

            if (in.readableBytes() < optionLength) {
                // TODO: 剩余可读长度小于optionLength，返回4.00客户端错误
                return;
            }
            byte[] optionValue = new byte[optionLength];
            in.readBytes(optionValue);  // TODO: 这里再确认一下readBytes会不会出问题

            options.add(new CoAPOption(optionNumber, optionValue));
        }

        // Handle payload
        byte[] payload = null;
        if (in.readableBytes() > 0) {
            payload = new byte[in.readableBytes()];
            in.readBytes(payload);
        }

        CoAPMessage coAPMessage = new CoAPMessage(version, type, tokenLength, code, messageId, token, options, payload);
        out.add(coAPMessage);
    }
}
