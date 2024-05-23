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

import com.alibaba.fastjson.JSON;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PUBLICATION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.USER_PROPERTY;
import static org.apache.rocketmq.mqtt.common.util.MessageUtil.EMPTYSTRING;
import static org.apache.rocketmq.mqtt.common.util.MessageUtil.dealEmptyMessage;
import static org.apache.rocketmq.mqtt.common.util.MessageUtil.removeRetainedFlag;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

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
        Assert.assertEquals(mqttPublishMessage.toString(), MessageUtil.toMqttMessage(topicName, messageBody.getBytes(), qos, mqttId, false).toString());
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

    @Test
    public void TestRemoveRetainedFlag() {
        ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);
        byte[] body = messageBody.getBytes(StandardCharsets.UTF_8);
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(body);
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qos), true, 0),
            new MqttPublishVariableHeader(topicName, mqttId), payload);
        MqttPublishMessage cleanRetainMqttPublishMessage = removeRetainedFlag(mqttPublishMessage);
        Assert.assertEquals(true, mqttPublishMessage.fixedHeader().isRetain());
        Assert.assertEquals(false, cleanRetainMqttPublishMessage.fixedHeader().isRetain());
    }

    @Test
    public void TestDealEmptyMessage() {
        messageBody = "";
        ByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);
        byte[] body = messageBody.getBytes(CharsetUtil.UTF_8);
        ByteBuf payload = ALLOCATOR.buffer();
        payload.writeBytes(body);
        mqttPublishMessage = new MqttPublishMessage(new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qos), true, 0),
            new MqttPublishVariableHeader(topicName, mqttId), payload);

        MqttPublishMessage newEmptyMessage = dealEmptyMessage(mqttPublishMessage);
        int readableBytes = newEmptyMessage.payload().readableBytes();
        byte[] newBody = new byte[readableBytes];
        newEmptyMessage.payload().readBytes(newBody);
        Assert.assertArrayEquals(EMPTYSTRING.getBytes(), newBody);
    }

    @Test
    public void TestMqtt5Message() {

        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.UserProperty("isSecret", "true"));
        props.add(new MqttProperties.UserProperty("tag", "firstTag"));
        props.add(new MqttProperties.UserProperty("tag", "secondTag"));

        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 100));
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 101));

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("test", 0, props);
        ByteBuf payload = Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8));
        MqttPublishMessage publishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        Message message = MessageUtil.toMessage(publishMessage);

        String mqtt5UserProperties = message.getUserProperty(Message.propertyMqtt5UserProperty);
        MqttPublishMessage newPublishMessage = null;

        if (StringUtils.isNotBlank(mqtt5UserProperties)) {
            ArrayList<MqttProperties.StringPair> userProperties = JSON.parseObject(mqtt5UserProperties,
                    new TypeReference<ArrayList<MqttProperties.StringPair>>() {}
            );
            MqttProperties newProps = new MqttProperties();
            newProps.add(new MqttProperties.UserProperties(userProperties));
            MqttPublishVariableHeader newVariableHeader = new MqttPublishVariableHeader("test", 0, props);
            newPublishMessage = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        }

        MqttProperties checkProps = newPublishMessage.variableHeader().properties();
        Assert.assertEquals("true", ((MqttProperties.StringPair)checkProps.getProperties(USER_PROPERTY.value()).get(0).value()).value);
        Assert.assertEquals("firstTag", ((MqttProperties.StringPair)checkProps.getProperties(USER_PROPERTY.value()).get(1).value()).value);
        Assert.assertEquals("secondTag", ((MqttProperties.StringPair)checkProps.getProperties(USER_PROPERTY.value()).get(2).value()).value);
    }
}
