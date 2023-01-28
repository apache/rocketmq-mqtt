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
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.GrpcRaftRpcFactory;
import com.alipay.sofa.jraft.rpc.impl.MarshallerRegistry;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.StoreMessage;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.meta.raft.rpc.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

import static org.apache.rocketmq.mqtt.meta.raft.rpc.Constants.GROUP_SEQ_NUM_SPLIT;
import static org.apache.rocketmq.mqtt.meta.raft.rpc.Constants.GROUP_RETAINED_MSG;


@Service
public class RetainedMsgClient {

    private static Logger logger = LoggerFactory.getLogger(RetainedMsgClient.class);
    private static final String GROUP_ID = GROUP_RETAINED_MSG + GROUP_SEQ_NUM_SPLIT + 0;
    final Configuration conf = new Configuration();
    static final CliClientServiceImpl CLICLIENTSERVICE = new CliClientServiceImpl();
    static PeerId leader;

    @Resource
    private ServiceConf serviceConf;

    @PostConstruct
    public void init() throws InterruptedException, TimeoutException {
        initRpcServer();
        if (!conf.parse(serviceConf.getMetaAddr())) {  //from service.conf
            throw new IllegalArgumentException("Fail to parse conf:" + serviceConf.getMetaAddr());
        }
        RouteTable.getInstance().updateConfiguration(GROUP_ID, conf);

        CLICLIENTSERVICE.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(CLICLIENTSERVICE, GROUP_ID, 3000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        leader = RouteTable.getInstance().selectLeader(GROUP_ID);
        logger.info("--------------------- Leader is " + leader + " ---------------------------");
    }

    public static void initRpcServer() {
        GrpcRaftRpcFactory raftRpcFactory = (GrpcRaftRpcFactory) RpcFactoryHelper.rpcFactory();
        raftRpcFactory.registerProtobufSerializer(WriteRequest.class.getName(), WriteRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(ReadRequest.class.getName(), ReadRequest.getDefaultInstance());
        raftRpcFactory.registerProtobufSerializer(Response.class.getName(), Response.getDefaultInstance());

        MarshallerRegistry registry = raftRpcFactory.getMarshallerRegistry();
        registry.registerResponseInstance(WriteRequest.class.getName(), Response.getDefaultInstance());
        registry.registerResponseInstance(ReadRequest.class.getName(), Response.getDefaultInstance());
    }

    public static void setRetainedMsg(String topic, Message msg, CompletableFuture<Boolean> future) throws RemotingException, InterruptedException {

        HashMap<String, String> option = new HashMap<>();
        option.put("topic", topic);
        option.put("firstTopic", msg.getFirstTopic());
        option.put("isEmpty", String.valueOf(msg.isEmpty()));

        logger.debug("SetRetainedMsg option:" + option);

        final WriteRequest request = WriteRequest.newBuilder().setGroup(GROUP_ID).setData(ByteString.copyFrom(msg.getEncodeBytes())).putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("SetRetainedMsg failed. {}", rsp.getErrMsg());
                        future.complete(false);
                        return;
                    }
                    logger.info("-------------------------------SetRetainedMsg success.----------------------------------");
                    future.complete(true);
                } else {
                    logger.debug("-------------------------------SetRetainedMsg fail.-------------------------------------");
                    logger.error("", err);
                    future.complete(false);
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);

    }

    public static void GetRetainedMsgsFromTrie(String firstTopic, String topic, CompletableFuture<ArrayList<Message>> future) throws RemotingException, InterruptedException {
        HashMap<String, String> option = new HashMap<>();

        option.put("firstTopic", firstTopic);
        option.put("topic", topic);

        logger.debug("GetRetainedMsgsFromTrie option:" + option);

        final ReadRequest request = ReadRequest.newBuilder().setGroup(GROUP_ID).setOperation("trie").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("GetRetainedTopicTrie failed. {}", rsp.getErrMsg());
                        future.complete(null);
                        return;
                    }
                    List<ByteString> datalistList = rsp.getDatalistList();
                    ArrayList<Message> resultList = new ArrayList<>();
                    for (ByteString tmp : datalistList) {
                        try {
                            resultList.add(Message.copyFromStoreMessage(StoreMessage.parseFrom(tmp.toByteArray())));
                        } catch (InvalidProtocolBufferException e) {
                            future.complete(null);
                            throw new RuntimeException(e);
                        }
                    }
                    future.complete(resultList);
                    logger.debug("-------------------------------GetRetainedTopicTrie success.----------------------------------");
                } else {
                    logger.debug("-------------------------------GetRetainedTopicTrie fail.-------------------------------------");
                    logger.error("", err);
                    future.complete(null);
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }


    public static void GetRetainedMsg(String topic, CompletableFuture<Message> future) throws RemotingException, InterruptedException {

        HashMap<String, String> option = new HashMap<>();
        option.put("topic", topic);

        final ReadRequest request = ReadRequest.newBuilder().setGroup(GROUP_ID).setOperation("topic").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("GetRetainedMsg failed. {}", rsp.getErrMsg());
                        future.complete(null);
                        return;
                    }
                    if (rsp.getData().toStringUtf8().equals("null")) {  //this topic doesn't exist retained msg
                        future.complete(null);
                        return;
                    }
                    Message message = null;
                    try {
                        message = Message.copyFromStoreMessage(StoreMessage.parseFrom(rsp.getData().toByteArray()));
                    } catch (InvalidProtocolBufferException e) {
                        future.complete(null);
                        throw new RuntimeException(e);
                    }
                    future.complete(message);
                } else {
                    logger.error("", err);
                    future.complete(null);
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }
}
