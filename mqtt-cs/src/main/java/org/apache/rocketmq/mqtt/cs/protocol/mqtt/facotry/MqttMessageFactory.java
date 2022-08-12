package org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;

import java.util.Objects;

/**
 * @author Austin Wong
 * Created on 2022/8/12 17:07:56
 */
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


}
