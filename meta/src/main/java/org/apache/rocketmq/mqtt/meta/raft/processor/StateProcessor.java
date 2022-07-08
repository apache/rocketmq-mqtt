package org.apache.rocketmq.mqtt.meta.raft.processor;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;

public abstract class StateProcessor {

    public abstract Response onReadRequest(ReadRequest request);

    public abstract Response onWriteRequest(WriteRequest log);

    public void onError(Throwable error) {
    }
    public abstract String groupCategory();

}
