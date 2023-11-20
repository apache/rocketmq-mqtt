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


package org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;

import java.util.Objects;

public class MqttMessageFactory {

    public static MqttConnAckMessage buildConnAckMessage(MqttConnectReturnCode returnCode) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(returnCode, false);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    public static MqttMessage buildPingRespMessage() {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(mqttFixedHeader);
    }

    public static MqttPublishMessage buildPublishMessage(String topicName, byte[] body, int qosLevel, int messageId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qosLevel), false, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, messageId);
        ByteBuf payload = Objects.isNull(body) ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body);
        return new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, payload);
    }

    public static MqttPubAckMessage buildPubAckMessage(Integer messageId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttPubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }

    public static MqttMessage buildPubRecMessage(Integer messageId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(messageId));
    }

    public static MqttMessage buildPubRelMessage(MqttMessageIdVariableHeader mqttMessageIdVariableHeader) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }

    public static MqttMessage buildPubCompMessage(MqttMessageIdVariableHeader mqttMessageIdVariableHeader) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        return new MqttMessage(mqttFixedHeader, mqttMessageIdVariableHeader);
    }

    public static MqttSubAckMessage buildSubAckMessage(Integer messageId, int... qosLevels) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader messageIdVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(qosLevels);
        return new MqttSubAckMessage(mqttFixedHeader, messageIdVariableHeader, mqttSubAckPayload);
    }

    public static MqttUnsubAckMessage buildUnsubAckMessage(Integer messageId) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader idVariableHeader = MqttMessageIdVariableHeader.from(messageId);
        return new MqttUnsubAckMessage(mqttFixedHeader, idVariableHeader);
    }


    public static MqttConnAckMessage createConnAckMessage(MqttConnectReturnCode mqttConnectReturnCode, Boolean sessionPresent) {
        return MqttMessageBuilders.connAck()
                .returnCode(mqttConnectReturnCode)
                .properties(MqttProperties.NO_PROPERTIES)
                .sessionPresent(sessionPresent)
                .build();
    }

    public static MqttConnAckMessage createConnAckMessage(MqttConnectReturnCode mqttConnectReturnCode,
                                                          Boolean sessionPresent, MqttProperties properties) {
        return MqttMessageBuilders.connAck()
                .returnCode(mqttConnectReturnCode)
                .properties(properties)
                .sessionPresent(sessionPresent)
                .build();
    }

}
