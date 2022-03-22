package org.apache.rocketmq.mqtt.common.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.*;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TestMessageUtil {

    String messageBody;
    String topicName;
    int qos;
    int mqttId;
    MqttPublishMessage mqttPublishMessage;


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
    }

    @Test
    public void TestToMqttMessage() {
        Assert.assertEquals(mqttPublishMessage.toString(), MessageUtil.toMqttMessage(topicName, messageBody.getBytes(), qos, mqttId).toString());
    }

    @Test
    public void TestToMessage() {
        Message message = new Message();
        message.setFirstTopic(topicName);
        message.putUserProperty(Message.extPropertyQoS, String.valueOf(qos));
        message.setPayload(messageBody.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(message, MessageUtil.toMessage(mqttPublishMessage));
    }
}
