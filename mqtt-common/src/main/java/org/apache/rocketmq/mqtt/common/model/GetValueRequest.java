package org.apache.rocketmq.mqtt.common.model;

import java.io.Serializable;

public class GetValueRequest implements Serializable {
    private static final long serialVersionUID = 9218253805003988802L;

    private boolean readOnlySafe = true;

    public boolean isReadOnlySafe() {
        return readOnlySafe;
    }

    public void setReadOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;
    }
}
