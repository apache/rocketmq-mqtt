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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.StoreMessage;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.common.meta.MetaConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

public class RetainedMsgClientTest {


    @Mock
    private Message testMsg = new Message();
    String firstTopic = "test-f1";

    String originTopic = "test-f1/f2/";

    String topicFilter = "test-f1/+/";
    final String groupId = MetaConstants.CATEGORY_RETAINED_MSG + "-" + 0;
    final String confStr = "127.0.0.1:25001";
    CliClientServiceImpl cliClientService = new CliClientServiceImpl();
    Configuration conf = new Configuration();
    PeerId leader;

    class RouteTableWrap {
        public boolean refreshLeader() throws InterruptedException, TimeoutException {
            return RouteTable.getInstance().refreshLeader(cliClientService, groupId, 3000).isOk();
        }

        public PeerId selectLeader(String groupId) {
            return RouteTable.getInstance().selectLeader(groupId);
        }
    }

    class RetainedMsgStateProcessWarp {
        public Response setRetainedMsgRsp() {
            return null;
        }

        public Response getRetainedMsgRsp() {
            return null;
        }

        public Response getRetainedMsgFromTrieRsp() {
            return null;
        }
    }

    @Before
    public void init() throws InterruptedException, TimeoutException {
        initRpcServer();

        if (!conf.parse(confStr)) {
            throw new IllegalArgumentException("Fail to parse conf:" + confStr);
        }

        RouteTable.getInstance().updateConfiguration(groupId, conf);

        cliClientService.init(new CliOptions());

        RouteTableWrap tmpRouteTable = Mockito.mock(RouteTableWrap.class);
        Mockito.when(tmpRouteTable.refreshLeader()).thenReturn(true);
        Mockito.when(tmpRouteTable.selectLeader(groupId)).thenReturn(new PeerId("127.0.0.1", 25001));

        if (!tmpRouteTable.refreshLeader()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        leader = tmpRouteTable.selectLeader(groupId);

        testMsg.setPayload("hello world".getBytes());
        testMsg.setMsgId("12345678");
        testMsg.setFirstTopic(firstTopic);
        testMsg.setOriginTopic(originTopic);
        testMsg.setEmpty(false);
        testMsg.setRetained(true);

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

    @Test
    public void TestSetRetainedMsg() {
        //test set retain msg

        HashMap<String, String> option = new HashMap<>();
        option.put("firstTopic", testMsg.getFirstTopic());
        option.put("topic", testMsg.getOriginTopic());
        option.put("isEmpty", String.valueOf(testMsg.isEmpty()));

        CompletableFuture<Boolean> future = new CompletableFuture<>();

        final WriteRequest request = WriteRequest.newBuilder().setGroup("retainedMsg-0").setOperation("topic").setData(ByteString.copyFrom(JSON.toJSONBytes(testMsg, SerializerFeature.WriteClassName))).putAllExtData(option).build();

        RetainedMsgStateProcessWarp stateProcess = Mockito.mock(RetainedMsgStateProcessWarp.class);
        Mockito.when(stateProcess.setRetainedMsgRsp()).thenReturn(Response.newBuilder()
            .setSuccess(true)
            .setData(ByteString.copyFrom(testMsg.getEncodeBytes()))
            .build());

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    Assert.assertEquals(stateProcess.setRetainedMsgRsp().getData().toStringUtf8(), request.getExtDataMap().get("topic"));
                    future.complete(stateProcess.setRetainedMsgRsp().getSuccess());
                }

                @Override
                public Executor executor() {
                    return null;
                }
            }, 5000);
        } catch (InterruptedException | RemotingException e) {
            throw new RuntimeException(e);
        }

        future.whenComplete(((result, throwable) -> {
            Assert.assertEquals(result, true);
        }));

    }

    @Test
    public void TestGetRetainedMsg() {

        HashMap<String, String> option = new HashMap<>();
        option.put("flag", "topic");
        option.put("topic", firstTopic + "/t1/");

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedmsg-0").setType(MetaConstants.READ_INDEX_TYPE).putAllExtData(option).build();

        CompletableFuture<Message> future = new CompletableFuture<>();

        RetainedMsgStateProcessWarp stateProcess = Mockito.mock(RetainedMsgStateProcessWarp.class);
        Mockito.when(stateProcess.getRetainedMsgRsp()).thenReturn(Response.newBuilder()
            .setSuccess(true)
            .setData(ByteString.copyFrom(testMsg.getEncodeBytes()))
            .build());

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    Response rsp = (Response) result;
                    Message msg = null;
                    try {
                        msg = Message.copyFromStoreMessage(StoreMessage.parseFrom(rsp.getData().toByteArray()));
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                    future.complete(msg);
                }

                @Override
                public Executor executor() {
                    return null;
                }
            }, 5000);
        } catch (InterruptedException | RemotingException e) {
            throw new RuntimeException(e);
        }

        future.whenComplete(((message, throwable) -> {
            Assert.assertEquals(message, testMsg);
        }));

    }

    @Test
    public void TestGetRetainedMsgsFromTrie() {
        //test get RetainedTopicTrie
        CompletableFuture<ArrayList<Message>> future = new CompletableFuture<>();

        HashMap<String, String> option = new HashMap<>();

        option.put("firstTopic", TopicUtils.normalizeTopic(firstTopic));
        option.put("topic", TopicUtils.normalizeTopic(topicFilter));


        ArrayList<ByteString> msgResults = new ArrayList<>();
        msgResults.add(ByteString.copyFrom(testMsg.getEncodeBytes()));
        msgResults.add(ByteString.copyFrom(testMsg.getEncodeBytes()));

        RetainedMsgStateProcessWarp stateProcess = Mockito.mock(RetainedMsgStateProcessWarp.class);
        Mockito.when(stateProcess.getRetainedMsgFromTrieRsp()).thenReturn(Response.newBuilder()
            .setSuccess(true)
            .addAllDatalist(msgResults)
            .build());

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedMsg-0").setOperation("trie").setType(MetaConstants.READ_INDEX_TYPE).putAllExtData(option).build();

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {

                    List<ByteString> datalistList = stateProcess.getRetainedMsgFromTrieRsp().getDatalistList();
                    ArrayList<Message> resultList = new ArrayList<>();
                    for (ByteString tmp : datalistList) {
                        try {
                            resultList.add(Message.copyFromStoreMessage(StoreMessage.parseFrom(tmp.toByteArray())));
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    future.complete(resultList);
                }

                @Override
                public Executor executor() {
                    return null;
                }
            }, 5000);
        } catch (InterruptedException | RemotingException e) {
            throw new RuntimeException(e);
        }

        ArrayList<Message> targetList = new ArrayList<>();
        targetList.add(testMsg);
        targetList.add(testMsg);

        future.whenComplete(((msgList, throwable) -> {
            Assert.assertEquals(msgList, targetList);
        }));

    }

}
