package org.apache.rocketmq.mqtt.meta.raft.processor;

import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;

public class CounterStateProcessor extends StateProcessor{


    @Override
    public Response onReadRequest(ReadRequest request) {
        return null;
    }

    @Override
    public Response onWriteRequest(WriteRequest log) {
        return null;
    }

    @Override
    public String groupCategory() {
        return Constants.COUNTER;
    }
}
