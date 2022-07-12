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

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;

public class MqttReadRpcProcessor extends AbstractRpcProcessor implements RpcProcessor<ReadRequest> {
    private final MqttRaftServer server;

    public MqttReadRpcProcessor(MqttRaftServer server) {
        this.server = server;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, ReadRequest request) {
        if (Constants.READ_INDEX_TYPE.equals(request.getType())) {
            handleReadIndex(server, request.getGroup(), rpcCtx, request);
        } else {
            handleRequest(server, request.getGroup(), rpcCtx, request);
        }

    }

    @Override
    public String interest() {
        return ReadRequest.class.getName();
    }
}
