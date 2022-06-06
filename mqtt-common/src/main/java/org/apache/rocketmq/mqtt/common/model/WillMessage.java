package org.apache.rocketmq.mqtt.common.model;

import java.util.Arrays;

public class WillMessage {

    private String willTopic;

    private byte[] body;

    private boolean isRetain;

    private int qos;

    public WillMessage(String willTopic, byte[] body, boolean isRetain, int qos) {
        this.willTopic = willTopic;
        this.body = body;
        this.isRetain = isRetain;
        this.qos = qos;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public boolean isRetain() {
        return isRetain;
    }

    public void setRetain(boolean retain) {
        isRetain = retain;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    @Override
    public String toString() {
        return "WillMessage{" +
                "willTopic='" + willTopic + '\'' +
                ", body=" + Arrays.toString(body) +
                ", isRetain=" + isRetain +
                ", qos=" + qos +
                '}';
    }
}
