package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;

public class MqttWriteRpcProcessor extends AbstractRpcProcessor implements RpcProcessor<ReadRequest> {
    private final MqttRaftServer server;

    public MqttWriteRpcProcessor(MqttRaftServer server) {
        this.server = server;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, ReadRequest request) {
        handleRequest(server, request.getGroup(), rpcCtx, request);
    }

    @Override
    public String interest() {
        return ReadRequest.class.getName();
    }
}
