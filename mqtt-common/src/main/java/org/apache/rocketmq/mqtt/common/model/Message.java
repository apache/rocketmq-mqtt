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

package org.apache.rocketmq.mqtt.common.model;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.model.consistency.StoreMessage;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class Message {
    private String msgId;
    private String firstTopic;
    private String originTopic;
    private long offset;
    private long nextOffset;
    private int retry;
    private boolean retained;
    private boolean isEmpty;
    private byte[] payload;
    private long bornTimestamp;
    private long storeTimestamp;
    private int ack = -1;
    private Map<String, String> userProperties = new HashMap<>();

    public static String propertyFirstTopic = "firstTopic";
    public static String propertyOriginTopic = "originTopic";
    public static String propertyOffset = "offset";
    public static String propertyNextOffset = "nextOffset";
    public static String propertyMsgId = "msgId";
    public static String propertyRetry = "retry";
    public static String propertyBornTime = "bornTime";
    public static String propertyStoreTime = "storeTime";
    public static String propertyUserProperties = "extData";

    public static String extPropertyMqttRealTopic = "mqttRealTopic";
    public static String extPropertyQoS = "qosLevel";
    public static String extPropertyCleanSessionFlag = "cleanSessionFlag";

    public static String extPropertyNamespaceId = "namespace";
    public static String extPropertyClientId = "clientId";


    public Message copy() {
        Message message = new Message();
        message.setMsgId(this.msgId);
        message.setFirstTopic(this.firstTopic);
        message.setOriginTopic(this.getOriginTopic());
        message.setOffset(this.getOffset());
        message.setNextOffset(this.getNextOffset());
        message.setRetry(this.getRetry());
        message.setPayload(this.getPayload());
        message.setBornTimestamp(this.bornTimestamp);
        message.setStoreTimestamp(this.storeTimestamp);
        message.setRetained(this.retained);
        message.setEmpty(this.isEmpty());
        message.getUserProperties().putAll(this.userProperties);
        return message;
    }

    public static Message copyFromStoreMessage(StoreMessage storeMessage) {
        Message message = new Message();
        message.setMsgId(storeMessage.getMsgId());
        message.setFirstTopic(storeMessage.getFirstTopic());
        message.setOriginTopic(storeMessage.getOriginTopic());
        message.setOffset(storeMessage.getOffset());
        message.setNextOffset(storeMessage.getNextOffset());
        message.setRetry(storeMessage.getRetry());
        message.setPayload(storeMessage.getPayload().toByteArray());
        message.setBornTimestamp(storeMessage.getBornTimestamp());
        message.setStoreTimestamp(storeMessage.getStoreTimestamp());
        message.setRetained(storeMessage.getRetained());
        message.setEmpty(storeMessage.getIsEmpty());
        message.getUserProperties().putAll(storeMessage.getUserPropertiesMap());
        return message;
    }

    public Integer qos() {
        if (getUserProperties() == null) {
            return null;
        }
        if (!getUserProperties().containsKey(extPropertyQoS)) {
            return null;
        }
        return Integer.parseInt(getUserProperties().get(extPropertyQoS));
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getFirstTopic() {
        return firstTopic;
    }

    public void setFirstTopic(String firstTopic) {
        this.firstTopic = firstTopic;
    }

    public String getOriginTopic() {
        return originTopic;
    }

    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getAck() {
        return ack;
    }

    public void setAck(int ack) {
        this.ack = ack;
    }

    public Map<String, String> getUserProperties() {
        return userProperties;
    }

    public void putUserProperty(String key, String value) {
        if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
            return;
        }
        userProperties.put(key, value);
    }

    public String getUserProperty(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        return userProperties.get(key);
    }

    public void clearUserProperty(String key) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        if (userProperties == null) {
            return;
        }
        userProperties.remove(key);
    }

    public boolean isP2P() {
        return TopicUtils.isP2P(TopicUtils.decode(firstTopic).getSecondTopic());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Message message = (Message) o;
        return offset == message.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset);
    }


    public boolean isRetained() {
        return retained;
    }

    public void setRetained(boolean retained) {
        this.retained = retained;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public void setEmpty(boolean empty) {
        isEmpty = empty;
    }

    public byte[] getEncodeBytes() {

        return StoreMessage.newBuilder()
            .setMsgId(this.getMsgId())
            .setFirstTopic(this.getFirstTopic())
            .setOriginTopic(this.getOriginTopic())
            .setOffset(this.getOffset())
            .setNextOffset(this.getNextOffset())
            .setRetry(this.getRetry())
            .setRetained(this.isRetained())
            .setIsEmpty(this.isEmpty())
            .setPayload(ByteString.copyFrom(this.getPayload()))
            .setBornTimestamp(this.getBornTimestamp())
            .setStoreTimestamp(this.getStoreTimestamp())
            .setAck(this.getAck())
            .putAllUserProperties(this.getUserProperties())
            .build().toByteString().toByteArray();
    }
}
