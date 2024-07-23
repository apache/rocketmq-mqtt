package org.apache.rocketmq.mqtt.common.model;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.net.InetSocketAddress;

public class CoapRequestMessage extends CoapMessage {

    private CoapRequestType requestType;
    private String topic;
    private String clientId;
    private MqttQoS qosLevel;
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
