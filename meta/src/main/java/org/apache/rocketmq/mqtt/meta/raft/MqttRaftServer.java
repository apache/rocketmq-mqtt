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

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import org.apache.rocketmq.mqtt.common.model.ValueResponse;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.raft.processor.GetValueRequestProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.IncrementAndGetRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;

@Service
public class MqttRaftServer {
    private static final Logger logger = LoggerFactory.getLogger(MqttRaftServer.class);

    @Resource
    private MetaConf metaConf;


    private PeerId localPeerId;
    private NodeOptions nodeOptions;
    private Configuration initConf;
    private RpcServer rpcServer;
    private Node node;
    private RaftGroupService raftGroupService;
    private MqttStateMachine fsm;

    @PostConstruct
    void init() {

        localPeerId = PeerId.parsePeer(metaConf.getSelfAddress());
        nodeOptions = new NodeOptions();

//        nodeOptions.setSharedElectionTimer(true);
//        nodeOptions.setSharedVoteTimer(true);
//        nodeOptions.setSharedStepDownTimer(true);
//        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setElectionTimeoutMs(1000);
//        nodeOptions.setEnableMetrics(true);
        nodeOptions.setSnapshotIntervalSecs(30);

        // Jraft implements parameter configuration internally. If you need to optimize, refer to https://www.sofastack.tech/projects/sofa-jraft/jraft-user-guide/
        RaftOptions raftOptions = new RaftOptions();
        nodeOptions.setRaftOptions(raftOptions);

        initConf = JRaftUtils.getConfiguration(metaConf.getMembersAddress());
        nodeOptions.setInitialConf(initConf);

        for (PeerId peerId:initConf.getPeers()) {
            NodeManager.getInstance().addAddress(peerId.getEndpoint());
        }

        // create rpc server to process mqtt request
//        rpcServer = RpcFactoryHelper.rpcFactory().createRpcServer(localPeerId.getEndpoint());
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(localPeerId.getEndpoint());
        // request mqtt rpc request processor
        MqttRaftService mqttRaftService = new MqttRaftServiceImpl(this);
        rpcServer.registerProcessor(new GetValueRequestProcessor(mqttRaftService));
        rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(mqttRaftService));

        // make raft reuse the rpc server
//        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer);
//        rpcServer.init(null);
//        if (!this.rpcServer.init(null)) {
//            logger.error("Fail to init [BaseRpcServer].");
//            throw new RuntimeException("Fail to init [BaseRpcServer].");
//        }

        this.fsm = new MqttStateMachine();
        nodeOptions.setFsm(this.fsm);
        String dataPath = metaConf.getRaftDataPath();
        // 日志, 必须
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        // 元信息, 必须
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        // snapshot, 可选, 一般都推荐
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        // 初始化 raft group 服务框架
        this.raftGroupService = new RaftGroupService("group0", this.localPeerId, nodeOptions, rpcServer);
        // 启动
        this.node = this.raftGroupService.start();
    }

    public Node getNode() {
        return this.node;
    }

    public RaftGroupService RaftGroupService() {
        return this.raftGroupService;
    }


    /**
     * Redirect request to new leader
     */
    public ValueResponse redirect() {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        if (this.node != null) {
            final PeerId leader = this.node.getLeaderId();
            if (leader != null) {
                response.setRedirect(leader.toString());
            }
        }
        return response;
    }

    public MqttStateMachine getFsm() {
        return this.fsm;
    }
}
