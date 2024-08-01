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

import io.netty.handler.codec.mqtt.MqttQoS;

import java.net.InetSocketAddress;

public class CoapRequestMessage extends CoapMessage {

    private CoapRequestType requestType;
    private String topic;
    private String clientId;
    private MqttQoS qosLevel = MqttQoS.AT_MOST_ONCE;
    private boolean isReatin;
    private int expiry;
    private String authToken;
    private String userName;
    private String password;


    public CoapRequestMessage(int version, CoapMessageType type, int tokenLength, CoapMessageCode code, int messageId, byte[] token, byte[] payload, InetSocketAddress remoteAddress) {
        super(version, type, tokenLength, code, messageId, token, payload, remoteAddress);
    }

    public CoapRequestMessage(int version, CoapMessageType type, int tokenLength, CoapMessageCode code, int messageId, byte[] token, InetSocketAddress remoteAddress) {
        super(version, type, tokenLength, code, messageId, token, remoteAddress);
    }

    public CoapRequestMessage copy() {
        CoapRequestMessage msg = new CoapRequestMessage(getVersion(), getType(), getTokenLength(), getCode(), getMessageId(), getToken(), getPayload(), getRemoteAddress());
        msg.setRequestType(requestType);
        msg.setTopic(topic);
        msg.setClientId(clientId);
        msg.setQosLevel(qosLevel);
        msg.setReatin(isReatin);
        msg.setExpiry(expiry);
        msg.setAuthToken(authToken);
        msg.setUserName(userName);
        msg.setPassword(password);
        return msg;
    }

    public CoapRequestType getRequestType() {
        return requestType;
    }

    public void setRequestType(CoapRequestType requestType) {
        this.requestType = requestType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public MqttQoS getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(MqttQoS qosLevel) {
        this.qosLevel = qosLevel;
    }

    public boolean isReatin() {
        return isReatin;
    }

    public void setReatin(boolean reatin) {
        isReatin = reatin;
    }

    public int getExpiry() {
        return expiry;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


}
