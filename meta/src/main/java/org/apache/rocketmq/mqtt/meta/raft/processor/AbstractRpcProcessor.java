/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.google.protobuf.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
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
                server.applyOperation(raftGroupHolder.getNode(), message, getFailoverClosure(rpcCtx));
            } else {
                rpcCtx.sendResponse(
                        Response.newBuilder().setSuccess(false).setErrMsg("Could not find leader : " + group).build());
            }
        } catch (Throwable e) {
            logger.error("handleRequest has error : ", e);
            rpcCtx.sendResponse(Response.newBuilder().setSuccess(false).setErrMsg(e.toString()).build());
        }
    }

    public FailoverClosure getFailoverClosure(final RpcContext rpcCtx) {
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
        return closure;
    }

    public void handleReadIndex(final MqttRaftServer server, final String group, final RpcContext rpcCtx, ReadRequest request) {
        try {
            final RaftGroupHolder raftGroupHolder = server.getRaftGroupHolder(group);
            if (Objects.isNull(raftGroupHolder)) {
                rpcCtx.sendResponse(Response.newBuilder().setSuccess(false)
                        .setErrMsg("Could not find the corresponding Raft Group : " + group).build());
                return;
            }

            final Node node = raftGroupHolder.getNode();
            final StateProcessor processor = raftGroupHolder.getMachine().getProcessor();

            try {
                node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                    @Override
                    public void run(Status status, long index, byte[] reqCtx) {
                        if (status.isOk()) {
                            try {
                                Response response = processor.onReadRequest(request);
                                rpcCtx.sendResponse(response);
                            } catch (Throwable t) {
                                logger.info("process read request in handleReadIndex error : {}", t.toString());
                                rpcCtx.sendResponse(Response.newBuilder().setErrMsg(t.toString()).setSuccess(false).build());
                            }
                            return;
                        }
                        logger.error("ReadIndex has error : {}, go to Leader read.", status.getErrorMsg());
                        readFromLeader(server, group, rpcCtx, request);
                    }
                });
            } catch (Throwable e) {
                logger.error("ReadIndex has error : {}, go to Leader read.", e.toString());
                // run raft read
                readFromLeader(server, group, rpcCtx, request);
            }

        } catch (Throwable e) {
            logger.error("handleReadIndex has error : ", e);
            rpcCtx.sendResponse(Response.newBuilder().setSuccess(false).setErrMsg(e.toString()).build());
        }
    }

    public void readFromLeader(final MqttRaftServer server, final String group, final RpcContext rpcCtx, ReadRequest request) {
        final RaftGroupHolder raftGroupHolder;
        try {
            raftGroupHolder = server.getRaftGroupHolder(group);
            if (Objects.isNull(raftGroupHolder)) {
                throw new Exception("can not get raft group");
            }
        } catch (Exception e) {
            rpcCtx.sendResponse(Response.newBuilder().setSuccess(false)
                    .setErrMsg("Could not find the corresponding Raft Group : " + group).build());
            return;
        }

        final Node node = raftGroupHolder.getNode();

        if (node.isLeader()) {
            server.applyOperation(raftGroupHolder.getNode(), request, getFailoverClosure(rpcCtx));
        } else {
            server.invokeToLeader(group, request, 5000, getFailoverClosure(rpcCtx));
        }
    }

}
