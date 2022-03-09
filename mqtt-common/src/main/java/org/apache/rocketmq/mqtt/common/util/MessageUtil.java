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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.mqtt.common.model.Message;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MessageUtil {
    public static final ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

    public static MqttPublishMessage toMqttMessage(String topicName, byte[] body, int qos, int mqttId) {
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(body);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false,
            MqttQoS.valueOf(qos),
            false, 0);
        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topicName, mqttId);
        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader,
            payload);
        return mqttPublishMessage;
    }

    public static Message toMessage(MqttPublishMessage mqttMessage) {
        Message message = new Message();
        message.setFirstTopic(TopicUtils.decode(mqttMessage.variableHeader().topicName()).getFirstTopic());
        message.setOriginTopic(mqttMessage.variableHeader().topicName());
        message.putUserProperty(Message.extPropertyQoS, String.valueOf(mqttMessage.fixedHeader().qosLevel().value()));
        int readableBytes = mqttMessage.payload().readableBytes();
        byte[] body = new byte[readableBytes];
        mqttMessage.payload().readBytes(body);
        message.setPayload(body);
        return message;
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
                    com.alibaba.fastjson.JSONObject.parseObject(ext, new TypeReference<Map<String, String>>() { }));
            }
            messageList.add(message);
        }
        return messageList;
    }

}
