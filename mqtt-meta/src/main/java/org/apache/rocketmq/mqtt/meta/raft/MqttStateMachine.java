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

package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.processor.StateProcessor;
import org.apache.rocketmq.mqtt.meta.rocksdb.RocksDBEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class MqttStateMachine extends StateMachineAdapter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttRaftServer.class);
    protected Node node;
    protected RocksDBEngine rocksDBEngine;
    protected final MqttRaftServer server;

    public MqttStateMachine(MqttRaftServer server) {
        this.server = server;
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

                    LOGGER.debug("get message:{} and apply to state machine", message);

                    if (message instanceof WriteRequest) {
                        WriteRequest writeRequest = (WriteRequest) message;
                        StateProcessor processor = server.getProcessor(writeRequest.getCategory());
                        Response response = processor.onWriteRequest((WriteRequest) message);
                        if (Objects.nonNull(closure)) {
                            closure.setResponse(response);
                        }
                    }

                    if (message instanceof ReadRequest) {
                        ReadRequest request = (ReadRequest) message;
                        StateProcessor processor = server.getProcessor(request.getCategory());
                        Response response = processor.onReadRequest((ReadRequest) message);
                        if (Objects.nonNull(closure)) {
                            closure.setResponse(response);
                        }
                    }
                    MqttApplyListener applyListener = server.getMqttApplyListener();
                    if (applyListener != null) {
                        applyListener.onApply(message);
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
            LOGGER.error("stateMachine meet critical error", t);
            //iterator.setErrorAndRollback(index - applied, new Status(RaftError.ESTATEMACHINE, "StateMachine meet critical error: %s.", t.toString()));
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        rocksDBEngine.getRocksDBSnapshot().onSnapshotSave(writer, done);
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return rocksDBEngine.getRocksDBSnapshot().onSnapshotLoad(reader);
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

    public void setNode(Node node) {
        this.node = node;
    }

    public Node getNode() {
        return node;
    }

    public void setRocksDBEngine(RocksDBEngine rocksDBEngine) {
        this.rocksDBEngine = rocksDBEngine;
    }

    public RocksDBEngine getRocksDBEngine() {
        return rocksDBEngine;
    }
}
