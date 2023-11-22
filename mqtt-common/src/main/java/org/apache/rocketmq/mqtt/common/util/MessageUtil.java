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

package org.apache.rocketmq.mqtt.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.mqtt.common.model.Message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.USER_PROPERTY;


public class MessageUtil {
    public static final ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

    public static final String EMPTYSTRING = "★\r\n\t☀";

    public static MqttPublishMessage toMqttMessage(String topicName, byte[] body, int qos, int mqttId, boolean retained) {
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(body);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false,
                MqttQoS.valueOf(qos),
                retained, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, mqttId);
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader,
                payload);
        return mqttPublishMessage;
    }

    public static Message toMessage(MqttPublishMessage mqttMessage) {
        Message message = new Message();
        message.setFirstTopic(TopicUtils.decode(mqttMessage.variableHeader().topicName()).getFirstTopic());
        message.setOriginTopic(mqttMessage.variableHeader().topicName());
        message.setRetained(mqttMessage.fixedHeader().isRetain());
        message.putUserProperty(Message.extPropertyQoS, String.valueOf(mqttMessage.fixedHeader().qosLevel().value()));
        int readableBytes = mqttMessage.payload().readableBytes();
        byte[] body = new byte[readableBytes];
        mqttMessage.payload().readBytes(body);
        message.setPayload(body);

        // MQTT PUBLISH Properties
        MqttPublishVariableHeader variableHeader = mqttMessage.variableHeader();
        MqttProperties mqttProperties = variableHeader.properties();
        if (mqttProperties != null) {
            Integer payloadFormatIndicator = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(PAYLOAD_FORMAT_INDICATOR.value())).value();
            if (payloadFormatIndicator != null) {
                message.putUserProperty(Message.propertyPayloadFormatIndicator, String.valueOf(payloadFormatIndicator));
            }

            // If absent, the Application Message does not expire.
            Integer messageExpiryInterval = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(PUBLICATION_EXPIRY_INTERVAL.value())).value();
            if (messageExpiryInterval != null) {
                message.putUserProperty(Message.propertyMessageExpiryInterval, String.valueOf(messageExpiryInterval));
            }

            Integer topicAlias = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(TOPIC_ALIAS.value())).value();
            if (topicAlias != null) {
                message.putUserProperty(Message.propertyTopicAlias, String.valueOf(topicAlias));
            }

            String responseTopic = ((MqttProperties.StringProperty) mqttProperties.getProperty(RESPONSE_TOPIC.value())).value();
            if (responseTopic != null) {
                message.putUserProperty(Message.propertyResponseTopic, responseTopic);
            }

            byte[] correlationData = ((MqttProperties.BinaryProperty) mqttProperties.getProperty(CORRELATION_DATA.value())).value();
            if (correlationData != null) {
                message.putUserProperty(Message.propertyCorrelationData, new String(correlationData, StandardCharsets.UTF_8));
            }

            // User Properties
            List<MqttProperties.UserProperty> userProperties = (List<MqttProperties.UserProperty>) mqttProperties.getProperties(USER_PROPERTY.value());
            List<MqttProperties.StringPair> userPropertyList = new ArrayList<>();
            for (MqttProperties.UserProperty userProperty : userProperties) {
                userPropertyList.add(userProperty.value());
            }

            if (!userPropertyList.isEmpty()) {
                message.putUserProperty(Message.propertyMqtt5UserProperty, JSON.toJSONString(userPropertyList));
            }

            List<MqttProperties.IntegerProperty> subscriptionIdentifier = (List<MqttProperties.IntegerProperty>) mqttProperties.getProperties(SUBSCRIPTION_IDENTIFIER.value());
            List<Integer> subscriptionIdentifierList = new ArrayList<>();
            for (MqttProperties.IntegerProperty sub : subscriptionIdentifier) {
                subscriptionIdentifierList.add(sub.value());
            }
            if (!subscriptionIdentifierList.isEmpty()) {
                message.putUserProperty(Message.propertySubscriptionIdentifier, JSON.toJSONString(subscriptionIdentifierList));
            }

            String contentType = ((MqttProperties.StringProperty) mqttProperties.getProperty(CONTENT_TYPE.value())).value();
            if (contentType != null) {
                message.putUserProperty(Message.propertyContentType, contentType);
            }
        }

        return message;
    }

    public static MqttPublishMessage removeRetainedFlag(MqttPublishMessage mqttPublishMessage) {
        MqttFixedHeader tmpFixHeader = mqttPublishMessage.fixedHeader();
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(tmpFixHeader.messageType(), tmpFixHeader.isDup(), tmpFixHeader.qosLevel(), false, tmpFixHeader.remainingLength()),
                mqttPublishMessage.variableHeader(),
                mqttPublishMessage.payload());
        return mqttPublishMessage;
    }

    public static MqttPublishMessage dealEmptyMessage(MqttPublishMessage mqttPublishMessage) {
        MqttFixedHeader tmpFixHeader = mqttPublishMessage.fixedHeader();
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(tmpFixHeader.messageType(), tmpFixHeader.isDup(), tmpFixHeader.qosLevel(), tmpFixHeader.isRetain(), tmpFixHeader.remainingLength()),
                mqttPublishMessage.variableHeader(),
                Unpooled.copiedBuffer(MessageUtil.EMPTYSTRING, CharsetUtil.UTF_8));
        return mqttPublishMessage;
    }

    public static byte[] encode(List<Message> messageList) {
        if (messageList == null || messageList.isEmpty()) {
            return null;
        }
        List<org.apache.rocketmq.common.message.Message> mqMessages = new ArrayList<>();
        for (Message message : messageList) {
            org.apache.rocketmq.common.message.Message mqMessage = new org.apache.rocketmq.common.message.Message();
            mqMessage.setBody(message.getPayload());
            mqMessage.putUserProperty(Message.propertyFirstTopic, message.getFirstTopic());
            if (message.getOriginTopic() != null) {
                mqMessage.putUserProperty(Message.propertyOriginTopic, message.getOriginTopic());
            }
            if (message.getMsgId() != null) {
                mqMessage.putUserProperty(Message.propertyMsgId, message.getMsgId());
            }
            mqMessage.putUserProperty(Message.propertyOffset, String.valueOf(message.getOffset()));
            mqMessage.putUserProperty(Message.propertyNextOffset, String.valueOf(message.getNextOffset()));
            mqMessage.putUserProperty(Message.propertyRetry, String.valueOf(message.getRetry()));
            mqMessage.putUserProperty(Message.propertyBornTime, String.valueOf(message.getBornTimestamp()));
            mqMessage.putUserProperty(Message.propertyStoreTime, String.valueOf(message.getStoreTimestamp()));
            mqMessage.putUserProperty(Message.propertyUserProperties,
                    JSONObject.toJSONString(message.getUserProperties()));
            mqMessages.add(mqMessage);
        }
        return MessageDecoder.encodeMessages(mqMessages);
    }

    public static List<Message> decode(ByteBuffer byteBuffer) throws Exception {
        List<org.apache.rocketmq.common.message.Message> mqMessages = MessageDecoder.decodeMessages(byteBuffer);
        if (mqMessages == null) {
            return null;
        }
        List<Message> messageList = new ArrayList<>();
        for (org.apache.rocketmq.common.message.Message mqMessage : mqMessages) {
            Message message = new Message();
            message.setFirstTopic(mqMessage.getUserProperty(Message.propertyFirstTopic));
            message.setOriginTopic(mqMessage.getUserProperty(Message.propertyOriginTopic));
            message.setPayload(mqMessage.getBody());
            message.setMsgId(mqMessage.getUserProperty(Message.propertyMsgId));
            message.setOffset(Long.parseLong(mqMessage.getUserProperty(Message.propertyOffset)));
            message.setNextOffset(Long.parseLong(mqMessage.getUserProperty(Message.propertyNextOffset)));
            message.setStoreTimestamp(Long.parseLong(mqMessage.getUserProperty(Message.propertyStoreTime)));
            message.setBornTimestamp(Long.parseLong(mqMessage.getUserProperty(Message.propertyBornTime)));
            message.setRetry(Integer.parseInt(mqMessage.getUserProperty(Message.propertyRetry)));
            String ext = mqMessage.getUserProperty(Message.propertyUserProperties);
            if (ext != null) {
                message.getUserProperties().putAll(
                        JSONObject.parseObject(ext, new TypeReference<Map<String, String>>() {
                        }));
            }
            messageList.add(message);
        }
        return messageList;
    }

}
