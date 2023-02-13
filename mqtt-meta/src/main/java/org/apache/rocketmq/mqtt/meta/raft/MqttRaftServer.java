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
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.meta.RaftUtil;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.raft.processor.HashKvStateProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.RetainedMsgStateProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.StateProcessor;
import org.apache.rocketmq.mqtt.meta.raft.processor.WillMsgStateProcessor;
import org.apache.rocketmq.mqtt.meta.raft.rpc.MqttReadRpcProcessor;
import org.apache.rocketmq.mqtt.meta.raft.rpc.MqttWriteRpcProcessor;
import org.apache.rocketmq.mqtt.meta.rocksdb.RocksDBEngine;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class MqttRaftServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttRaftServer.class);

    @Resource
    private MetaConf metaConf;

    private static ExecutorService raftExecutor;
    private static ExecutorService requestExecutor;
    private static ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private PeerId localPeerId;
    private RpcServer rpcServer;
    private CliClientServiceImpl cliClientService;
    private CliService cliService;
    private Map<String, StateProcessor> stateProcessors = new ConcurrentHashMap<>();
    private Map<String, MqttStateMachine> bizStateMachineMap = new ConcurrentHashMap<>();
    public String[] raftGroups;
    private RouteTable rt;

    @PostConstruct
    void init() throws IOException, RocksDBException {
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

        registerStateProcessor(new RetainedMsgStateProcessor(this, metaConf.getMaxRetainedTopicNum()));  //add retained msg processor
        registerStateProcessor(new WillMsgStateProcessor(this));
        registerStateProcessor(new HashKvStateProcessor(this));

        rt = RouteTable.getInstance();
        localPeerId = PeerId.parsePeer(metaConf.getSelfAddress());
        rpcServer = createRpcServer(this, localPeerId);
        NodeManager.getInstance().addAddress(localPeerId.getEndpoint());
        if (!rpcServer.init(null)) {
            LOGGER.error("Fail to init [BaseRpcServer].");
            throw new RuntimeException("Fail to init [BaseRpcServer].");
        }

        raftGroups = RaftUtil.LIST_RAFT_GROUPS();
        for (String group : raftGroups) {
            String rdbPath = RaftUtil.RAFT_BASE_DIR(group) + File.separator + "rdb";
            FileUtils.forceMkdir(new File(rdbPath));
            RocksDBEngine rocksDBEngine = new RocksDBEngine(rdbPath);
            rocksDBEngine.init();
            MqttStateMachine sm = new MqttStateMachine(this);
            sm.setRocksDBEngine(rocksDBEngine);
            createRaftNode(group, sm);
        }
        scheduler.scheduleAtFixedRate(() -> refreshLeader(), 3, 3, TimeUnit.SECONDS);
        CliOptions cliOptions = new CliOptions();
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOptions);
        this.cliClientService = (CliClientServiceImpl) ((CliServiceImpl) this.cliService).getCliClientService();
    }

    private void refreshLeader() {
        for (String groupId : raftGroups) {
            try {
                rt.refreshLeader(cliClientService, groupId, 1000);
            } catch (Exception e) {
                LOGGER.error("refreshLeader failed {}", groupId, e);
            }
        }
    }

    public Node createRaftNode(String groupId, MqttStateMachine sm) throws IOException {
        if (StringUtils.isBlank(groupId) || sm == null) {
            throw new IllegalArgumentException("groupId or sm is null");
        }
        String dataPath = RaftUtil.RAFT_BASE_DIR(groupId);
        FileUtils.forceMkdir(new File(dataPath));

        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(metaConf.getElectionTimeoutMs());
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(metaConf.getSnapshotIntervalSecs());

        final Configuration initConf = new Configuration();
        String initConfStr = metaConf.getMembersAddress();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        rt.updateConfiguration(groupId, initConfStr);
        nodeOptions.setInitialConf(initConf);
        nodeOptions.setFsm(sm);
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");

        Node node = RaftServiceFactory.createAndInitRaftNode(groupId, localPeerId, nodeOptions);
        sm.setNode(node);
        registerBizStateMachine(groupId, sm);
        LOGGER.warn("createdRaftNode {}", groupId);
        return node;
    }

    private void registerBizStateMachine(String groupId, MqttStateMachine sm) {
        MqttStateMachine prv = bizStateMachineMap.putIfAbsent(groupId, sm);
        if (prv != null) {
            throw new RuntimeException("dup register BizStateMachine:" + groupId);
        }
    }

    public Node getNode(String groupId) {
        return bizStateMachineMap.get(groupId).getNode();
    }

    public MqttStateMachine getMqttStateMachine(String groupId) {
        return bizStateMachineMap.get(groupId);
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
        stateProcessors.put(processor.groupCategory(), processor);
    }

    public StateProcessor getProcessor(String category) {
        return stateProcessors.get(category);
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
        return rt.selectLeader(raftGroupId);
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
