package org.apache.rocketmq.mqtt.cs.protocol.coap;

public class CoAPOption {
    private int optionNumber;
    private byte[] optionValue;

    public CoAPOption(int optionNumber, byte[] optionValue) {
        this.optionNumber = optionNumber;
        this.optionValue = optionValue;
    }


    public int getOptionNumber() {
        return optionNumber;
    }

    public void setOptionNumber(int optionNumber) {
        this.optionNumber = optionNumber;
    }

    public byte[] getOptionValue() {
        return optionValue;
    }

    public void setOptionValue(byte[] optionValue) {
        this.optionValue = optionValue;
    }
}
