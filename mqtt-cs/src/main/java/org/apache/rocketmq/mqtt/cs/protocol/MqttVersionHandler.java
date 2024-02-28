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
package org.apache.rocketmq.mqtt.cs.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;

@ChannelHandler.Sharable
public class MqttVersionHandler extends ChannelInboundHandlerAdapter {

    private static final int MAX_REMAINING_LENGTH_MULTIPLIER = 0x80 * 0x80 * 0x80;
    private static final int NOT_ENOUGH_BYTES_READABLE = -2;
    private static final int MALFORMED_REMAINING_LENGTH = -1;
    private static final int MIN_FIXED_HEADER_LENGTH = 2;

    private ChannelPipelineLazyInit channelPipelineLazyInit;
    private ChannelManager channelManager;

    public MqttVersionHandler(ChannelManager channelManager, ChannelPipelineLazyInit channelPipelineLazyInit) {
        this.channelPipelineLazyInit = channelPipelineLazyInit;
        this.channelManager = channelManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (ChannelInfo.getMqttVersion(ctx.channel()) == null) {
            ByteBuf buf = (ByteBuf)msg;
            try {
                buf.markReaderIndex();
                final byte fixedHeader = buf.readByte();

                final int remainingLength = calculateRemainingLength(buf);

                if (remainingLength == NOT_ENOUGH_BYTES_READABLE) {
                    return;
                }

                if (remainingLength == MALFORMED_REMAINING_LENGTH) {
                    channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, "invalid remainingLength");
                    return;
                }
                MqttVersion mqttVersion = readAndSetMqttVersion(ctx, buf);
                if (mqttVersion == null) {
                    return;
                }
                channelPipelineLazyInit.init(ctx.pipeline(), mqttVersion);
            } finally {
                buf.resetReaderIndex();
            }
        }
        ctx.fireChannelRead(msg);
    }

    private MqttVersion readAndSetMqttVersion(ChannelHandlerContext ctx, ByteBuf buf) {
        if (buf.readableBytes() < 2) {
            channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, "CONNECT without protocol version");
            return null;
        }

        final ByteBuf lengthLSBBuf = buf.slice(buf.readerIndex() + 1, 1);

        final int lengthLSB = lengthLSBBuf.readByte();

        final MqttVersion protocolVersion;
        switch (lengthLSB) {
            case 4:
                if (buf.readableBytes() < 7) {
                    channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, "invalid protocol version");
                    return null;
                }
                final ByteBuf protocolVersionBuf = buf.slice(buf.readerIndex() + 6, 1);
                final byte versionByte = protocolVersionBuf.readByte();
                if (versionByte == 5) {
                    protocolVersion = MqttVersion.MQTT_5;
                } else if (versionByte == 4) {
                    protocolVersion = MqttVersion.MQTT_3_1_1;
                } else {
                    channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, "invalid protocol version");
                    return null;
                }
                break;
            case 6:
                protocolVersion = MqttVersion.MQTT_3_1;
                break;
            default:
                channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, "invalid protocol version");
                return null;
        }
        ChannelInfo.setMqttVersion(ctx.channel(), protocolVersion);
        return protocolVersion;
    }

    /**
     * Calculates the remaining length according to the MQTT spec.
     *
     * @param buf the message buffer
     * @return the remaining length, -1 if the remaining length is malformed or -2 if not enough bytes are available
     */
    private int calculateRemainingLength(final ByteBuf buf) {

        int remainingLength = 0;
        int multiplier = 1;
        byte encodedByte;

        do {
            if (multiplier > MAX_REMAINING_LENGTH_MULTIPLIER) {
                buf.skipBytes(buf.readableBytes());
                //This means the remaining length is malformed!
                return MALFORMED_REMAINING_LENGTH;
            }

            if (!buf.isReadable()) {
                return NOT_ENOUGH_BYTES_READABLE;
            }

            encodedByte = buf.readByte();

            remainingLength += (encodedByte & (byte)0x7f) * multiplier;
            multiplier *= 0x80;

        } while ((encodedByte & 0x80) != 0);

        return remainingLength;
    }
}
