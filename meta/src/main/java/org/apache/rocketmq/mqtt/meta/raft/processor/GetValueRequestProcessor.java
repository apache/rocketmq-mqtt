package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.apache.rocketmq.mqtt.common.model.GetValueRequest;
import org.apache.rocketmq.mqtt.meta.raft.MqttClosure;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftService;

public class GetValueRequestProcessor implements RpcProcessor<GetValueRequest> {

    private final MqttRaftService mqttRaftService;

    public GetValueRequestProcessor(MqttRaftService mqttRaftService) {
        super();
        this.mqttRaftService = mqttRaftService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final GetValueRequest request) {
        final MqttClosure closure = new MqttClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.mqttRaftService.get(request.isReadOnlySafe(), closure);
    }

    @Override
    public String interest() {
        return GetValueRequest.class.getName();
    }
}
