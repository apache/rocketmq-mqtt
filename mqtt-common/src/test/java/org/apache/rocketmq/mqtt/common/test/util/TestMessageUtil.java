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

package org.apache.rocketmq.mqtt.common.test.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TestMessageUtil {

    String messageBody;
    String topicName;
    int qos;
    int mqttId;
    MqttPublishMessage mqttPublishMessage;
    Message message;
    List<Message> messageList;

    @Before
    public void Before() {
        messageBody = "Hello-mqtt";
        topicName = "topicTest";
        qos = 0;
        mqttId = 1;
        ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);
        byte[] body = messageBody.getBytes(StandardCharsets.UTF_8);
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(body);
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qos), false, 0),
                new MqttPublishVariableHeader(topicName, mqttId), payload);

        message = new Message();
        message.setFirstTopic(topicName);
        message.putUserProperty(Message.extPropertyQoS, String.valueOf(qos));
        message.setPayload(messageBody.getBytes(StandardCharsets.UTF_8));
        messageList = new ArrayList<>();
        messageList.add(message);
    }

    @Test
    public void TestToMqttMessage() {
        Assert.assertEquals(mqttPublishMessage.toString(), MessageUtil.toMqttMessage(topicName, messageBody.getBytes(), qos, mqttId,false).toString());
    }

    @Test
    public void TestToMessage() {
        Assert.assertEquals(message, MessageUtil.toMessage(mqttPublishMessage));
    }

    @Test
    public void TestEncodeAndDecode() throws Exception {
        byte[] bytes = MessageUtil.encode(messageList);
        List<Message> decodeMsgList = MessageUtil.decode(ByteBuffer.wrap(bytes));
        Assert.assertEquals(1, decodeMsgList.size());
        Assert.assertEquals(message, decodeMsgList.get(0));
    }
}
