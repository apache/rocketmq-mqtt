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
package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageOption;

import java.util.List;

public class CoapEncoder extends MessageToMessageEncoder<CoapMessage> {

    @Override
    public void encode(ChannelHandlerContext ctx, CoapMessage msg, List<Object> out) throws Exception {

        ByteBuf buffer = Unpooled.buffer();

        // Handle Version | Type | TokenLength
        byte firstByte = (byte)((msg.getVersion() << 6) | (msg.getType().value() << 4) | (msg.getTokenLength() & 0x0F));
        buffer.writeByte(firstByte);

        // Handle Code, MessageID, Token
        buffer.writeByte(msg.getCode().value());
        buffer.writeShort(msg.getMessageId());
        buffer.writeBytes(msg.getToken());

        // Handle Options
        if (!msg.getOptions().isEmpty()) {
            int prevOptionNumber = 0;
            for (CoapMessageOption option : msg.getOptions()) {
                int optionDelta = option.getOptionNumber().value() - prevOptionNumber;
                prevOptionNumber = option.getOptionNumber().value();
                int optionLength = option.getOptionValue() == null ? 0 : option.getOptionValue().length;

                if (optionDelta < 13) {
                    buffer.writeByte((byte)((optionDelta << 4) | (optionLength & 0x0F)));
                } else if (optionDelta < 269) {
                    buffer.writeByte((byte)((13 << 4) | (optionLength & 0x0F)));
                    buffer.writeByte(optionDelta - 13);
                } else {
                    buffer.writeByte((byte)((14 << 4) | (optionLength & 0x0F)));
                    buffer.writeShort(optionDelta - 269);
                }

                if (optionLength > 12 && optionLength < 269) {
                    buffer.writeByte(optionLength - 13);
                } else if (optionLength >= 269) {
                    buffer.writeShort(optionLength - 269);
                }

                if (optionLength > 0) {
                    buffer.writeBytes(option.getOptionValue());
                }
            }
        }

        // Handle Payload if not empty
        if (msg.getPayload() != null && msg.getPayload().length > 0) {
            buffer.writeByte((byte)0xFF);
            buffer.writeBytes(msg.getPayload());
        }

        // Send Response
        DatagramPacket responsePacket = new DatagramPacket(buffer, msg.getRemoteAddress());
        out.add(responsePacket);

    }

}