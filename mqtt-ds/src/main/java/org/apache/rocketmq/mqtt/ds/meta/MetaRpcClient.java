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

package org.apache.rocketmq.mqtt.ds.meta;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MetaRpcClient {
    private static Logger logger = LoggerFactory.getLogger(MetaRpcClient.class);
    private Configuration conf = new Configuration();
    private CliClientServiceImpl cliClientService = new CliClientServiceImpl();
    private String raftGroupId;
    private static ScheduledExecutorService raftClientExecutor = Executors.newSingleThreadScheduledExecutor();

    public MetaRpcClient(String metaAddress, String raftGroupId) {
        this.raftGroupId = raftGroupId;

        initRpcServer();
        if (!conf.parse(metaAddress)) {
            throw new IllegalArgumentException("Fail to parse conf:" + metaAddress + "for raftGroupId:" + raftGroupId);
        }
        RouteTable.getInstance().updateConfiguration(raftGroupId, conf);
        cliClientService.init(new CliOptions());
        raftClientExecutor.scheduleAtFixedRate(()-> {
            try {
                refreshLeader();
            } catch (Exception e) {
                logger.error("raftGroupId: {} refresh leader error", raftGroupId, e);
            }
        }, 0, 3000, TimeUnit.MILLISECONDS);
    }

    public void initRpcServer() {
        GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
        raftRpcFactory.registerProtobufSerializer(WriteRequest.class.getName(), WriteRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(ReadRequest.class.getName(), ReadRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(Response.class.getName(), Response.getDefaultInstance());

        MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
        registry.registerResponseInstance(WriteRequest.class.getName(), Response.getDefaultInstance());
        registry.registerResponseInstance(ReadRequest.class.getName(), Response.getDefaultInstance());
    }

    public void updateRaftGroupIdConfiguration(Configuration conf) {
        this.conf = conf;
        RouteTable.getInstance().updateConfiguration(raftGroupId, this.conf);
    }

    public void refreshLeader() throws InterruptedException, TimeoutException {
        if (!RouteTable.getInstance().refreshLeader(cliClientService, raftGroupId, 3000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }
    }

    public PeerId getLeader() {
        return RouteTable.getInstance().selectLeader(raftGroupId);
    }

    public CliClientServiceImpl getCliClientService() {
        return cliClientService;
    }
}
