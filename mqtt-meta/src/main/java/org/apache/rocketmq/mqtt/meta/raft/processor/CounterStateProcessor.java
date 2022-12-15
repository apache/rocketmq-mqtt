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

import com.alibaba.fastjson.JSON;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.rpc.Constants;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.SnapshotOperation;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.impl.CounterSnapshotOperation;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public class CounterStateProcessor extends StateProcessor {

    private final AtomicLong value = new AtomicLong(0);
    
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    private final SnapshotOperation snapshotOperation;

    public CounterStateProcessor() {
        this.snapshotOperation = new CounterSnapshotOperation(lock);
    }

    @Override
    public Response onReadRequest(ReadRequest request) {
        try {
            return Response.newBuilder()
                .setSuccess(true)
                .setData(ByteString.copyFrom(JSON.toJSONBytes(value.toString())))
                .build();
        } catch (Exception e) {
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }
    }

    @Override
    public Response onWriteRequest(WriteRequest writeRequest) {

        try {
            Long delta = Long.parseLong(writeRequest.getExtDataMap().get("delta"));
            Long res = value.addAndGet(delta);
            return Response.newBuilder()
                .setSuccess(true)
                .setData(ByteString.copyFrom(JSON.toJSONBytes(res.toString())))
                .build();
        } catch (Exception e) {
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally) {
        snapshotOperation.onSnapshotSave(writer, callFinally, value.toString());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        String load = snapshotOperation.onSnapshotLoad(reader);
        value.set(Long.parseLong(load));
        return true;
    }

    @Override
    public String groupCategory() {
        return Constants.COUNTER;
    }
}
