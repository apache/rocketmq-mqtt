package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.google.protobuf.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.processor.StateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class MqttStateMachine extends StateMachineAdapter {
    private static final Logger logger = LoggerFactory.getLogger(MqttRaftServer.class);

    protected final MqttRaftServer server;

    protected final StateProcessor processor;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final String groupId;

    private Node node;

    private volatile long term = -1;

    private volatile String leaderIp = "unknown";

    public MqttStateMachine(MqttRaftServer server, StateProcessor processor, String groupId) {
        this.server = server;
        this.processor = processor;
        this.groupId = groupId;
    }


    @Override
    public void onApply(Iterator iterator) {
        int index = 0;
        int applied = 0;
        Message message;
        MqttClosure closure = null;
        try {
            while (iterator.hasNext()) {
                Status status = Status.OK();
                try {
                    if (iterator.done() != null) {
                        closure = (MqttClosure) iterator.done();
                        message = closure.getMessage();
                    } else {
                        final ByteBuffer data = iterator.getData();
                        message = parseMessage(data.array());
                    }

                    logger.debug("get message:{} and apply to state machine", message);

                    if (message instanceof WriteRequest) {
                        Response response = processor.onWriteRequest((WriteRequest) message);
                        if (Objects.nonNull(closure)) {
                            closure.setResponse(response);
                        }
                    }

                    if (message instanceof ReadRequest) {
                        Response response = processor.onReadRequest((ReadRequest) message);
                        if (Objects.nonNull(closure)) {
                            closure.setResponse(response);
                        }
                    }
                } catch (Throwable e) {
                    index++;
                    status.setError(RaftError.UNKNOWN, e.toString());
                    Optional.ofNullable(closure).ifPresent(closure1 -> closure1.setThrowable(e));
                    throw e;
                } finally {
                    Optional.ofNullable(closure).ifPresent(closure1 -> closure1.run(status));
                }

                applied++;
                index++;
                iterator.next();
            }
        } catch (Throwable t) {
            logger.error("processor : {}, stateMachine meet critical error: {}.", processor, t);
            iterator.setErrorAndRollback(index - applied,
                    new Status(RaftError.ESTATEMACHINE, "StateMachine meet critical error: %s.", t.toString()));
        }
    }

    public Message parseMessage(byte[] bytes) throws Exception {
        Message result;
        try {
            result = WriteRequest.parseFrom(bytes);
            return result;
        } catch (Throwable ignore) {
        }
        try {
            result = ReadRequest.parseFrom(bytes);
            return result;
        } catch (Throwable ignore) {
        }

        throw new Exception("parse message from bytes error");
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.term = term;
        this.isLeader.set(true);
        this.leaderIp = node.getNodeId().getPeerId().getEndpoint().toString();
    }

    public void setNode(Node node) {
        this.node = node;
    }
}
