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

package org.apache.rocketmq.mqtt.common.test.model;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.StoreMessage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TestMessage {

    String msgId = "testMsgCopy";
    String firstTopic = "first";
    String originTopic = "origin";
    long offset = 0;
    long nextOffset = 1;
    int retry = 3;
    byte[] payload = "testMsgCopy".getBytes(StandardCharsets.UTF_8);
    long bornTimestamp = 100;
    long storeTimestamp = 120;
    int extPropertyQoS = 2;

    @Test
    public void testMessageCopy() throws InvalidProtocolBufferException {
        Message message = new Message();
        message.setMsgId(msgId);
        message.setFirstTopic(firstTopic);
        message.setOriginTopic(originTopic);
        message.setOffset(offset);
        message.setNextOffset(nextOffset);
        message.setRetry(retry);
        message.setPayload(payload);
        message.setBornTimestamp(bornTimestamp);
        message.setStoreTimestamp(storeTimestamp);
        message.putUserProperty(Message.extPropertyQoS, String.valueOf(extPropertyQoS));

        Message copyMsg = message.copy();

        Assert.assertEquals(msgId, copyMsg.getMsgId());
        Assert.assertEquals(firstTopic, copyMsg.getFirstTopic());
        Assert.assertEquals(originTopic, copyMsg.getOriginTopic());
        Assert.assertEquals(offset, copyMsg.getOffset());
        Assert.assertEquals(nextOffset, copyMsg.getNextOffset());
        Assert.assertEquals(retry, copyMsg.getRetry());
        Assert.assertEquals(payload, copyMsg.getPayload());
        Assert.assertEquals(bornTimestamp, copyMsg.getBornTimestamp());
        Assert.assertEquals(storeTimestamp, copyMsg.getStoreTimestamp());
        Assert.assertEquals(String.valueOf(extPropertyQoS), copyMsg.getUserProperty(Message.extPropertyQoS));
        Assert.assertNull(copyMsg.getUserProperty(Message.propertyMsgId));
        copyMsg.clearUserProperty(Message.extPropertyQoS);
        Assert.assertNull(copyMsg.getUserProperty(Message.extPropertyQoS));


        StoreMessage storeMessage = StoreMessage.newBuilder()
            .setMsgId(message.getMsgId())
            .setFirstTopic(message.getFirstTopic())
            .setOriginTopic(message.getOriginTopic())
            .setOffset(message.getOffset())
            .setNextOffset(message.getNextOffset())
            .setRetry(message.getRetry())
            .setRetained(message.isRetained())
            .setIsEmpty(message.isEmpty())
            .setPayload(ByteString.copyFrom(message.getPayload()))
            .setBornTimestamp(message.getBornTimestamp())
            .setStoreTimestamp(message.getStoreTimestamp())
            .setAck(message.getAck())
            .putAllUserProperties(message.getUserProperties())
            .build();

        byte[] bytes = storeMessage.toByteString().toByteArray();

        StoreMessage tmpStoreMessage = StoreMessage.parseFrom(bytes);

        Assert.assertEquals(storeMessage, tmpStoreMessage);


    }
}
