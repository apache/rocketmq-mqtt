package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.apache.rocketmq.mqtt.common.model.IncrementAndGetRequest;
import org.apache.rocketmq.mqtt.meta.raft.MqttClosure;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftService;

public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final MqttRaftService mqttRaftService;

    public IncrementAndGetRequestProcessor(MqttRaftService mqttRaftService) {
        super();
        this.mqttRaftService = mqttRaftService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final IncrementAndGetRequest request) {
        final MqttClosure closure = new MqttClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.mqttRaftService.incrementAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
