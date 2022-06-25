package org.apache.rocketmq.mqtt.common.model;

import java.io.Serializable;

public class IncrementAndGetRequest implements Serializable {
    private static final long serialVersionUID = -5623664785560971849L;

    private long delta;

    public long getDelta() {
        return this.delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }
}
