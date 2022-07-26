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

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.SnapshotOperation;

import java.util.function.BiConsumer;

/**
 * A concrete processing class for a business state machine
 */
public abstract class StateProcessor {

    /**
     * Process the read request to apply the state machine
     * @param request
     * @return
     */
    public abstract Response onReadRequest(ReadRequest request);

    /**
     * Process the write request to apply the state machine
     * @param log
     * @return
     */
    public abstract Response onWriteRequest(WriteRequest log);

    public SnapshotOperation loadSnapshotOperate() {
        return null;
    }

    /**
     * Save the state machine snapshot
     * @param writer
     * @param callFinally
     */
    public abstract void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally);

    /**
     * Load the state machine snapshot
     * @param reader
     * @return
     */
    public abstract boolean onSnapshotLoad(SnapshotReader reader);

    public void onError(Throwable error) {
    }

    /**
     * Raft Grouping category. The grouping category and sequence number identify the unique RAFT group
     * @return
     */
    public abstract String groupCategory();

}
