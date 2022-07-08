package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.google.protobuf.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.meta.raft.FailoverClosure;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;
import org.apache.rocketmq.mqtt.meta.raft.RaftGroupHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public abstract class AbstractRpcProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRpcProcessor.class);

    protected void handleRequest(final MqttRaftServer server, final String group, final RpcContext rpcCtx, Message message) {
        try {
            final RaftGroupHolder raftGroupHolder = server.getRaftGroupHolder(group);
            if (Objects.isNull(raftGroupHolder)) {
                rpcCtx.sendResponse(Response.newBuilder().setSuccess(false)
                        .setErrMsg("Could not find the corresponding Raft Group : " + group).build());
                return;
            }
            if (raftGroupHolder.getNode().isLeader()) {
                FailoverClosure closure = new FailoverClosure() {

                    Response data;

                    Throwable ex;

                    @Override
                    public void setResponse(Response data) {
                        this.data = data;
                    }

                    @Override
                    public void setThrowable(Throwable throwable) {
                        this.ex = throwable;
                    }

                    @Override
                    public void run(Status status) {
                        if (Objects.nonNull(ex)) {
                            logger.error("execute has error : ", ex);
                            rpcCtx.sendResponse(Response.newBuilder().setErrMsg(ex.toString()).setSuccess(false).build());
                        } else {
                            rpcCtx.sendResponse(data);
                        }
                    }
                };

                server.applyOperation(raftGroupHolder.getNode(), message, closure);
            } else {
                rpcCtx.sendResponse(
                        Response.newBuilder().setSuccess(false).setErrMsg("Could not find leader : " + group).build());
            }
        } catch (Throwable e) {
            logger.error("handleRequest has error : ", e);
            rpcCtx.sendResponse(Response.newBuilder().setSuccess(false).setErrMsg(e.toString()).build());
        }
    }

}
