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

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.raft.processor.CounterStateProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.MqttReadRpcProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.MqttWriteRpcProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.StateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class MqttRaftServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttRaftServer.class);

    @Resource
    private MetaConf metaConf;

    private static ExecutorService raftExecutor;
    private static ExecutorService requestExecutor;


    private PeerId localPeerId;
    private NodeOptions nodeOptions;
    private Configuration initConf;
    private RpcServer rpcServer;

    private CliClientServiceImpl cliClientService;

    private CliService cliService;
    private Map<String, List<RaftGroupHolder>> multiRaftGroup = new ConcurrentHashMap<>();
    private Collection<StateProcessor> stateProcessors = Collections.synchronizedSet(new HashSet<>());

    @PostConstruct
    void init() {
        raftExecutor = new ThreadPoolExecutor(
                8,
                16,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryImpl("RaftExecutor_"));
        requestExecutor = new ThreadPoolExecutor(
                8,
                16,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryImpl("requestExecutor_"));

        localPeerId = PeerId.parsePeer(metaConf.getSelfAddress());
        nodeOptions = new NodeOptions();

        nodeOptions.setSharedElectionTimer(true);
        nodeOptions.setSharedVoteTimer(true);
        nodeOptions.setSharedStepDownTimer(true);
        nodeOptions.setSharedSnapshotTimer(true);
        nodeOptions.setElectionTimeoutMs(metaConf.getElectionTimeoutMs());
        nodeOptions.setEnableMetrics(true);
        nodeOptions.setSnapshotIntervalSecs(metaConf.getSnapshotIntervalSecs());

        // Jraft implements parameter configuration internally. If you need to optimize, refer to https://www.sofastack.tech/projects/sofa-jraft/jraft-user-guide/
        RaftOptions raftOptions = new RaftOptions();
        nodeOptions.setRaftOptions(raftOptions);

        initConf = JRaftUtils.getConfiguration(metaConf.getMembersAddress());
        nodeOptions.setInitialConf(initConf);

        for (PeerId peerId : initConf.getPeers()) {
            NodeManager.getInstance().addAddress(peerId.getEndpoint());
        }

        rpcServer = createRpcServer(this, localPeerId);
        if (!this.rpcServer.init(null)) {
            LOGGER.error("Fail to init [BaseRpcServer].");
            throw new RuntimeException("Fail to init [BaseRpcServer].");
        }

        CliOptions cliOptions = new CliOptions();
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOptions);
        this.cliClientService = (CliClientServiceImpl) ((CliServiceImpl) this.cliService).getCliClientService();

        registerStateProcessor(new CounterStateProcessor());
        start();
    }

    public RpcServer createRpcServer(MqttRaftServer server, PeerId peerId) {
        GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
        raftRpcFactory.registerProtobufSerializer(WriteRequest.class.getName(), WriteRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(ReadRequest.class.getName(), ReadRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(Response.class.getName(), Response.getDefaultInstance());

        MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
        registry.registerResponseInstance(WriteRequest.class.getName(), Response.getDefaultInstance());
        registry.registerResponseInstance(ReadRequest.class.getName(), Response.getDefaultInstance());

        final RpcServer rpcServer = raftRpcFactory.createRpcServer(peerId.getEndpoint());
        RaftRpcServerFactory.addRaftRequestProcessors(rpcServer, raftExecutor, requestExecutor);

        rpcServer.registerProcessor(new MqttWriteRpcProcessor(server));
        rpcServer.registerProcessor(new MqttReadRpcProcessor(server));

        return rpcServer;
    }

    public void registerStateProcessor(StateProcessor processor) {
        stateProcessors.add(processor);
    }

    public void start() {

        int eachProcessRaftGroupNum = metaConf.getRaftGroupNum();
        for (StateProcessor processor : stateProcessors) {

            List<RaftGroupHolder> raftGroupHolderList = multiRaftGroup.get(processor.groupCategory());
            if (raftGroupHolderList == null) {
                raftGroupHolderList = new ArrayList<>();
                List<RaftGroupHolder> raftGroupHolderListOld = multiRaftGroup.putIfAbsent(processor.groupCategory(), raftGroupHolderList);
                if (raftGroupHolderListOld != null) {
                    raftGroupHolderList = raftGroupHolderListOld;
                }
            }

            for (int i = 0; i < eachProcessRaftGroupNum; ++i) {
                String groupIdentity = wrapGroupName(processor.groupCategory(), i);

                Configuration groupConfiguration = initConf.copy();
                // LogUri RaftMetaUri SnapshotUri will not be copy, so need to be set
                NodeOptions groupNodeOption = nodeOptions.copy();
                initStoreUri(groupIdentity, groupNodeOption);

                MqttStateMachine groupMqttStateMachine = new MqttStateMachine(this, processor, groupIdentity);
                groupNodeOption.setFsm(groupMqttStateMachine);
                groupNodeOption.setInitialConf(groupConfiguration);

                // to-do: snapshot
                int doSnapshotInterval = 0;
                groupNodeOption.setSnapshotIntervalSecs(doSnapshotInterval);

                // create raft group
                RaftGroupService raftGroupService = new RaftGroupService(groupIdentity, localPeerId, groupNodeOption, rpcServer, true);

                // start node
                Node node = raftGroupService.start(false);
                groupMqttStateMachine.setNode(node);
                RouteTable.getInstance().updateConfiguration(groupIdentity, groupConfiguration);

                // to-do : a new node start, it can be added to this group, without restart running server. maybe need refresh route tables
                // add to multiRaftGroup
                raftGroupHolderList.add(new RaftGroupHolder(raftGroupService, groupMqttStateMachine, node));

                LOGGER.info("create raft group, groupIdentity: {}", groupIdentity);
            }
        }
    }

    private void initStoreUri(String groupIdentity, NodeOptions nodeOptions) {
        String dataPath = metaConf.getRaftDataPath();

        String logUri = dataPath + File.separator + groupIdentity + File.separator + "log";
        String raftMetaUri = dataPath + File.separator + groupIdentity + File.separator + "raft_meta";
        String snapshotUri = dataPath + File.separator + groupIdentity + File.separator + "snapshot";

        try {
            FileUtils.forceMkdir(new File(logUri));
            FileUtils.forceMkdir(new File(raftMetaUri));
            FileUtils.forceMkdir(new File(snapshotUri));
        } catch (Exception e) {
            LOGGER.error("create dir for raft store uri error, e:{}", e.toString());
            throw new RuntimeException(e);
        }

        nodeOptions.setLogUri(logUri);
        nodeOptions.setRaftMetaUri(raftMetaUri);
        nodeOptions.setSnapshotUri(snapshotUri);
    }

    private String wrapGroupName(String category, int seq) {
        return category + "-" + seq;
    }

    public RaftGroupHolder getRaftGroupHolder(String groupId) throws Exception {
        String[] groupParam = groupId.split("%");
        if (groupParam.length != 2) {
            throw new Exception("Fail to get RaftGroupHolder");
        }

        return multiRaftGroup.get(groupParam[0]).get(Integer.parseInt(groupParam[1]));
    }

    public void applyOperation(Node node, Message data, FailoverClosure closure) {
        final Task task = new Task();
        MqttClosure mqttClosure = new MqttClosure(data, status -> {
            MqttClosure.MqttStatus mqttStatus = (MqttClosure.MqttStatus) status;
            closure.setThrowable(mqttStatus.getThrowable());
            closure.setResponse(mqttStatus.getResponse());
            closure.run(mqttStatus);
        });

        task.setData(ByteBuffer.wrap(data.toByteArray()));
        task.setDone(mqttClosure);
        node.apply(task);
    }

    protected PeerId getLeader(final String raftGroupId) {
        return RouteTable.getInstance().selectLeader(raftGroupId);
    }

    public void invokeToLeader(final String group, final Message request, final int timeoutMillis,
                               FailoverClosure closure) {
        try {
            final PeerId peerId = getLeader(group);
            final Endpoint leaderIp = peerId.getEndpoint();
            cliClientService.getRpcClient().invokeAsync(leaderIp, request, new InvokeCallback() {
                @Override
                public void complete(Object o, Throwable ex) {
                    if (Objects.nonNull(ex)) {
                        closure.setThrowable(ex);
                        closure.run(new Status(RaftError.UNKNOWN, ex.getMessage()));
                        return;
                    }
                    if (!((Response) o).getSuccess()) {
                        closure.setThrowable(new IllegalStateException(((Response) o).getErrMsg()));
                        closure.run(new Status(RaftError.UNKNOWN, ((Response) o).getErrMsg()));
                        return;
                    }
                    closure.setResponse((Response) o);
                    closure.run(Status.OK());
                }

                @Override
                public Executor executor() {
                    return requestExecutor;
                }
            }, timeoutMillis);
        } catch (Exception e) {
            closure.setThrowable(e);
            closure.run(new Status(RaftError.UNKNOWN, e.toString()));
        }
    }
}
