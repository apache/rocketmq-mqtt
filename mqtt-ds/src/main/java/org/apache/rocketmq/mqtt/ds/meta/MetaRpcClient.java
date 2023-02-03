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

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.meta.RaftUtil;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.rocketmq.mqtt.common.meta.RaftUtil.RAFT_GROUP_NUM;

@Component
public class MetaRpcClient {
    private static Logger logger = LoggerFactory.getLogger(MetaRpcClient.class);
    private RouteTable rt;
    private Configuration conf;
    private CliClientServiceImpl cliClientService;
    private static ScheduledExecutorService raftClientExecutor = Executors.newSingleThreadScheduledExecutor();
    public String[] raftGroups;

    @Resource
    private ServiceConf serviceConf;

    @PostConstruct
    public void init() throws InterruptedException, TimeoutException {
        initRpcServer();
        cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        rt = RouteTable.getInstance();
        conf = JRaftUtils.getConfiguration(serviceConf.getMetaAddr());
        raftGroups = RaftUtil.LIST_RAFT_GROUPS();
        for (String groupId : raftGroups) {
            rt.updateConfiguration(groupId, conf);
        }
        refreshLeader();
        raftClientExecutor.scheduleAtFixedRate(() -> refreshLeader(), 3, 3, TimeUnit.SECONDS);
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

    private void refreshLeader() {
        for (String groupId : raftGroups) {
            try {
                rt.refreshLeader(cliClientService, groupId, 1000);
            } catch (Exception e) {
                logger.error("refreshLeader failed {}", groupId, e);
            }
        }
    }

    public PeerId getLeader(String raftGroupId) {
        return rt.selectLeader(raftGroupId);
    }

    public CliClientServiceImpl getCliClientService() {
        return cliClientService;
    }

    public String whichGroup(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        int index = key.hashCode() % RAFT_GROUP_NUM;
        if (index < 0) {
            index = 0;
        }
        return raftGroups[index];
    }

    public String[] getRaftGroups() {
        return raftGroups;
    }
}
