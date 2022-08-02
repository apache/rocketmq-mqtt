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

package org.apache.rocketmq.mqtt.ds.retain;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.meta.raft.processor.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;



@Service
public class RetainedMsgClient {

    private static Logger logger = LoggerFactory.getLogger(RetainedMsgClient.class);
    private static final String GROUP_SEQ_NUM_SPLIT = "-";
    final String groupId = Constants.RETAINEDMSG + GROUP_SEQ_NUM_SPLIT + 0;
    final Configuration conf = new Configuration();
    static final CliClientServiceImpl CLICLIENTSERVICE = new CliClientServiceImpl();

    static final String GROUPNAME = "retainedmsg";
    static PeerId leader;

    @Resource
    private ServiceConf serviceConf;

    @PostConstruct
    public void init() throws InterruptedException, TimeoutException {
        initRpcServer();
        if (!conf.parse(serviceConf.getMetaAddr())) {  //from service.conf
            throw new IllegalArgumentException("Fail to parse conf:" + serviceConf.getMetaAddr());
        }
        RouteTable.getInstance().updateConfiguration(groupId, conf);

        CLICLIENTSERVICE.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(CLICLIENTSERVICE, groupId, 3000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        leader = RouteTable.getInstance().selectLeader(groupId);
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

    public static void SetRetainedMsg(String topic, Message msg, CompletableFuture<Boolean> future) throws RemotingException,
        InterruptedException {

        HashMap<String, String> option = new HashMap<>();
        option.put("message", JSON.toJSONString(msg, SerializerFeature.WriteClassName));
        option.put("topic", topic);
        logger.info("SetRetainedMsg option:" + option.toString());

        final WriteRequest request = WriteRequest.newBuilder().setGroup(GROUPNAME + "-0").putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("SetRetainedMsg failed. {}", rsp.getErrMsg());
                        return;
                    }
                    logger.info("-------------------------------SetRetainedMsg success.----------------------------------");
                    future.complete(true);
                } else {
                    logger.info("-------------------------------SetRetainedMsg fail.-------------------------------------");
                    future.complete(false);
                    err.printStackTrace();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    public static void GetRetainedTopicTrie(String firstTopic, CompletableFuture<Trie<String, String>> future) throws RemotingException,
        InterruptedException {
        HashMap<String, String> option = new HashMap<>();

        option.put("flag", "trie");  //request for trie
        option.put("topic", firstTopic);


        final ReadRequest request = ReadRequest.newBuilder().setGroup(GROUPNAME + "-0").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("GetRetainedTopicTrie failed. {}", rsp.getErrMsg());
                        return;
                    }
                    Trie<String, String> tmpTrie = JSON.parseObject(rsp.getData().toStringUtf8(), Trie.class);
                    logger.info("GetRetainedTopicTrie success.Trie :" + tmpTrie);
                    future.complete(tmpTrie);
                    logger.info("-------------------------------GetRetainedTopicTrie success.----------------------------------");
                } else {
                    logger.info("-------------------------------GetRetainedTopicTrie fail.-------------------------------------");
                    future.complete(null);
                    err.printStackTrace();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    public static void GetRetainedMsg(String topic, CompletableFuture<Message> future) throws RemotingException,
        InterruptedException {

        HashMap<String, String> option = new HashMap<>();
        option.put("flag", "topic");  //request for retain msg of topic
        option.put("topic", topic);


        final ReadRequest request = ReadRequest.newBuilder().setGroup(GROUPNAME + "-0").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        CLICLIENTSERVICE.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (rsp.getData().toStringUtf8().equals("null")) {
                        logger.info("GetRetainedMsg failed. {}", rsp.getErrMsg());
                        return;
                    }
                    String strMsg = (String) JSON.parse(rsp.getData().toStringUtf8());
                    Message message = JSON.parseObject(strMsg, Message.class);
                    future.complete(message);
                } else {
                    future.complete(null);
                    err.printStackTrace();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }


}
