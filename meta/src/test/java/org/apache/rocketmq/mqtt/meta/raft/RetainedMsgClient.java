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
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.meta.raft.processor.Constants;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class RetainedMsgClient {

    public static void main(final String[] args) throws Exception {

        final String groupId = Constants.RETAINEDMSG + "-" + 0;
        final String confStr = "127.0.0.1:25001";

        initRpcServer();
        final Configuration conf = new Configuration();
        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 3000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
        System.out.println("Leader is " + leader);

        //test set retain msg
        Message msg1 = new Message();
        msg1.setPayload("hello world".getBytes());
        msg1.setMsgId("1111111");
        msg1.setFirstTopic("yutao-f1");
        msg1.setOriginTopic("yutao-f1/t1/");
        msg1.setEmpty(false);
        msg1.setRetained(true);
        CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
        setRetainedMsg(cliClientService, leader, msg1, future);
        future.whenComplete((res, throwable) -> {
            System.out.println("future get:" + res);
        });

        //test get msg
        CompletableFuture<Message> future1 = new CompletableFuture<Message>();
        getRetainedMsg(cliClientService, leader, "yutao-f1/t1/", future1);

        future1.whenComplete((msg, throwable) -> {
            System.out.println("msgfuture get: " + msg.getOriginTopic());
        });

        //test get trie
        CompletableFuture<Trie<String, String>> future2 = new CompletableFuture<Trie<String, String>>();
        getRetainedTopicTrie(cliClientService, leader, "yutao-f2", future2);

        future2.whenComplete((trie, throwable) -> {
            System.out.println("triefuture get: " + trie);
        });


        Thread.sleep(1000);
        System.exit(0);
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

    private static void setRetainedMsg(final CliClientServiceImpl cliClientService, final PeerId leader, Message msg, CompletableFuture<Boolean> future) throws RemotingException,
        InterruptedException {

        HashMap<String, String> option = new HashMap<>();

        option.put("message", JSON.toJSONString(msg, SerializerFeature.WriteClassName));
        option.put("topic", msg.getOriginTopic());
        System.out.println(JSON.toJSONString(msg, SerializerFeature.WriteClassName));


        final WriteRequest request = WriteRequest.newBuilder().setGroup("retainedmsg-0").putAllExtData(option).build();

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {

                    System.out.println("set result:" + result);
                    future.complete(true);
                } else {
                    err.printStackTrace();

                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void getRetainedTopicTrie(final CliClientServiceImpl cliClientService, final PeerId leader, String firstTopic, CompletableFuture<Trie<String, String>> future) throws RemotingException,
        InterruptedException {
        HashMap<String, String> option = new HashMap<>();

        option.put("flag", "trie");
        option.put("topic", TopicUtils.normalizeTopic(firstTopic));

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedmsg-0").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {

                    Response rsp = (Response) result;

                    if (!rsp.getSuccess()) {
                        System.out.println("error");
                        return;
                    }
                    Trie<String, String> tmpTrie = JSON.parseObject(rsp.getData().toStringUtf8(), Trie.class);
                    System.out.println(tmpTrie);
                    future.complete(tmpTrie);

                } else {
                    err.printStackTrace();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }

    private static void getRetainedMsg(final CliClientServiceImpl cliClientService, final PeerId leader, String topic, CompletableFuture<Message> future) throws RemotingException,
        InterruptedException {

        HashMap<String, String> option = new HashMap<>();
        option.put("flag", "topic");
        option.put("topic", topic);

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedmsg-0").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {

                    Response rsp = (Response) result;
                    if (rsp.getData().toStringUtf8().equals("null")) {
                        System.out.println("empty msg");
                        return;
                    }
                    String strMsg = (String) JSON.parse(rsp.getData().toStringUtf8());
                    Message message = JSON.parseObject(strMsg, Message.class);
                    System.out.println(new String(message.getPayload()));
                    future.complete(message);
                } else {
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
