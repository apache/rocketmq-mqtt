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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageOption;
import org.apache.rocketmq.mqtt.common.model.CoapMessageOptionNumber;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.CoapRequestType;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapResponseCache;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;



public class CoapDecoder extends MessageToMessageDecoder<DatagramPacket> {

    @Resource
    private CoapResponseCache coapResponseCache;

    private CoapMessageType coapType;
    private int coapTokenLength;
    private CoapMessageCode coapCode;
    private int coapMessageId;
    private byte[] coapToken;
    private byte[] coapPayload;
    InetSocketAddress remoteAddress;
    private boolean isObserve;

    private String errorContent;
    private CoapMessageCode errorCode;

    @Override
    public void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> out) {

        ByteBuf in = packet.content();
        remoteAddress = packet.sender();

        // The length of Coap message is at least 4 bytes.
        if (in.readableBytes() < 4) {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The length of header must be at least 4 bytes!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }

        // Handle first byte, including version, type, and token length.
        int firstByte = in.readUnsignedByte();
        int version = (firstByte >> 6) & 0x03;
        if (version != Constants.COAP_VERSION) {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: Version must be 1!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }
        coapType = CoapMessageType.valueOf((firstByte >> 4) & 0x03);
        coapTokenLength = firstByte & 0x0F;
        if (coapTokenLength > Constants.COAP_MAX_TOKEN_LENGTH) {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The length of token is too long!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }

        // Handle code
        try {
            coapCode = CoapMessageCode.valueOf(in.readUnsignedByte());
        } catch (IllegalArgumentException e) {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The code is not defined!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }
        if (!CoapMessageCode.isRequestCode(coapCode) && !CoapMessageCode.isEmptyCode(coapCode)) {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The code must be a request code!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }

        // Handle messageID
        coapMessageId = in.readUnsignedShort();

        // Handle token
        if (in.readableBytes() < coapTokenLength) {
            // Return 4.00 Response
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The length of remaining readable bytes is less than tokenLength!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }
        coapToken = new byte[coapTokenLength];
        in.readBytes(coapToken);

        CoapRequestMessage coapMessage = new CoapRequestMessage(version, coapType, coapTokenLength, coapCode, coapMessageId, coapToken, remoteAddress);

        // Handle ACK
        if (coapType == CoapMessageType.ACK) {
            coapMessage.setRequestType(CoapRequestType.ACK);
            ctx.fireChannelRead(coapMessage);
            return;
        }

        // Handle options
        int nextByte;
        int optionNumber = 0;
        List<String> uriPaths = new ArrayList<>();
        while (in.readableBytes() > 0) {

            nextByte = in.readUnsignedByte();
            if (nextByte == Constants.COAP_PAYLOAD_MARKER) {
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
                errorCode = CoapMessageCode.BAD_REQUEST;
                errorContent = "Format-Error: OptionDelta can not be 15!";
                sendErrorResponse(ctx);
                in.skipBytes(in.readableBytes());
                return;
            }

            optionNumber += optionDelta;    // current optionNumber = last optionNumber + optionDelta

            if (!CoapMessageOptionNumber.isValid(optionNumber)) {
                // Return 4.02 Response
                errorCode = CoapMessageCode.BAD_OPTION;
                errorContent = "Format-Error: Option number is not defined!";
                sendErrorResponse(ctx);
                in.skipBytes(in.readableBytes());
                return;
            }

            if (optionLength == 13) {
                optionLength += in.readUnsignedByte();
            } else if (optionLength == 14) {
                optionLength += 255 + in.readUnsignedShort();
            } else if (optionLength == 15) {
                // Return 4.00 Response
                errorCode = CoapMessageCode.BAD_REQUEST;
                errorContent = "Format-Error: OptionLength can not be 15!";
                sendErrorResponse(ctx);
                in.skipBytes(in.readableBytes());
                return;
            }

            if (in.readableBytes() < optionLength) {
                // Return 4.00 Response
                errorCode = CoapMessageCode.BAD_REQUEST;
                errorContent = "Format-Error: The number of readable bytes is less than optionLength";
                sendErrorResponse(ctx);
                in.skipBytes(in.readableBytes());
                return;
            }
            byte[] optionValue = new byte[optionLength];
            in.readBytes(optionValue);

            if (optionNumber == CoapMessageOptionNumber.URI_PATH.value()) {
                uriPaths.add(new String(optionValue, StandardCharsets.UTF_8));
            }

            if (optionNumber == CoapMessageOptionNumber.URI_QUERY.value()) {
                String query = new String(optionValue, StandardCharsets.UTF_8);
                String[] parts = query.split(Constants.COAP_QUERY_DELIMITER, 2);
                if (parts.length != 2) {
                    // Return 4.00 Response
                    errorCode = CoapMessageCode.BAD_REQUEST;
                    errorContent = "Format-Error: The Format of Observe is not correct!";
                    sendErrorResponse(ctx);
                    // Skip unread bytes
                    in.skipBytes(in.readableBytes());
                    return;
                }
                switch (parts[0]) {
                    case Constants.COAP_QUERY_CLIENT_ID:
                        coapMessage.setClientId(parts[1]);
                        break;
                    case Constants.COAP_QUERY_QOS:
                        coapMessage.setQosLevel(MqttQoS.valueOf(Integer.parseInt(parts[1])));
                        break;
                    case Constants.COAP_QUERY_RETAIN:
                        coapMessage.setReatin(Boolean.parseBoolean(parts[1]));
                        break;
                    case Constants.COAP_QUERY_EXPIRY:
                        coapMessage.setExpiry(Integer.parseInt(parts[1]));
                        break;
                    case Constants.COAP_QUERY_USER_NAME:
                        coapMessage.setUserName(parts[1]);
                    case Constants.COAP_QUERY_PASSWORD:
                        coapMessage.setPassword(parts[1]);
                    default:
                        break;
                }
            }

            if (optionNumber == CoapMessageOptionNumber.OBSERVE.value()) {
                if (optionValue.length == 0) {
                    isObserve = true;
                } else {
                    // Return 4.00 Response
                    errorCode = CoapMessageCode.BAD_REQUEST;
                    errorContent = "Format-Error: The Format of Observe is not correct!";
                    sendErrorResponse(ctx);
                    // Skip unread bytes
                    in.skipBytes(in.readableBytes());
                    return;
                }
            }

            coapMessage.addOption(new CoapMessageOption(optionNumber, optionValue));
        }

        if (uriPaths.isEmpty()) {
            // Return 4.00 Response
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The Format is not correct!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }

        // Handle Uri-Path
        if (uriPaths.get(0).equals(Constants.COAP_PS_PREFIX)) {
            switch (coapCode) {
                case GET:
                    if (isObserve) {
                        coapMessage.setRequestType(CoapRequestType.SUBSCRIBE);
                    } else {
                        errorCode = CoapMessageCode.BAD_REQUEST;
                        errorContent = "Format-Error: The Format is not correct!";
                        sendErrorResponse(ctx);
                        // Skip unread bytes
                        in.skipBytes(in.readableBytes());
                        return;
                    }
                    break;
                case POST:
                    coapMessage.setRequestType(CoapRequestType.PUBLISH);
                    break;
                default:
                    errorCode = CoapMessageCode.BAD_REQUEST;
                    errorContent = "Format-Error: The Format is not correct!";
                    sendErrorResponse(ctx);
                    // Skip unread bytes
                    in.skipBytes(in.readableBytes());
                    return;
            }

            // construct topic
            coapMessage.setTopic(uriPaths.stream().skip(1).collect(Collectors.joining(Constants.MQTT_TOPIC_DELIMITER)));

        } else if (uriPaths.size() == 2 && uriPaths.get(0).equals(Constants.COAP_CONNECTION_PREFIX_1) && uriPaths.get(1).equals(Constants.COAP_CONNECTION_PREFIX_2)) {
            switch (coapCode) {
                case POST:
                    coapMessage.setRequestType(CoapRequestType.CONNECT);
                    break;
                case DELETE:
                    coapMessage.setRequestType(CoapRequestType.DISCONNECT);
                    break;
                case PUT:
                    coapMessage.setRequestType(CoapRequestType.HEARTBEAT);
                    break;
                default:
                    errorCode = CoapMessageCode.BAD_REQUEST;
                    errorContent = "Format-Error: The Format is not correct!";
                    sendErrorResponse(ctx);
                    // Skip unread bytes
                    in.skipBytes(in.readableBytes());
                    return;
            }
        } else {
            errorCode = CoapMessageCode.BAD_REQUEST;
            errorContent = "Format-Error: The Format is not correct!";
            sendErrorResponse(ctx);
            // Skip unread bytes
            in.skipBytes(in.readableBytes());
            return;
        }

        // Handle payload
        if (in.readableBytes() > 0) {
            coapPayload = new byte[in.readableBytes()];
            in.readBytes(coapPayload);
            coapMessage.setPayload(coapPayload);
        }

//        sendTestResponse(ctx);
        out.add(coapMessage);
    }

    public void sendErrorResponse(ChannelHandlerContext ctx) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                coapType == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                coapToken == null ? 0 : coapTokenLength,
                errorCode,
                coapMessageId,
                coapToken,
                errorContent.getBytes(StandardCharsets.UTF_8),
                remoteAddress
        );
        ctx.writeAndFlush(response);
        coapResponseCache.put(response);
    }

}