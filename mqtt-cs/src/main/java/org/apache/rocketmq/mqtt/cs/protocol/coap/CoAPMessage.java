package org.apache.rocketmq.mqtt.cs.protocol.coap;

import java.net.InetSocketAddress;
import java.util.List;

public class CoAPMessage {
    private int version;
    private int type;
    private int tokenLength;
    private int code;
    private int messageId;
    private byte[] token;
    private List<CoAPOption> options;
    private byte[] payload;
    private InetSocketAddress remoteAddress;

    public CoAPMessage(int version, int type, int tokenLength, int code, int messageId, byte[] token, List<CoAPOption> options, byte[] payload, InetSocketAddress remoteAddress) {
        this.version = version;
        this.type = type;
        this.tokenLength = tokenLength;
        this.code = code;
        this.messageId = messageId;
        this.token = token;
        this.options = options;
        this.payload = payload;
        this.remoteAddress = remoteAddress;
    }


    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getTokenLength() {
        return tokenLength;
    }

    public void setTokenLength(int tokenLength) {
        this.tokenLength = tokenLength;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
    }

    public List<CoAPOption> getOptions() {
        return options;
    }

    public void setOptions(List<CoAPOption> options) {
        this.options = options;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public void setRemoteAddress(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }
}
