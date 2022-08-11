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
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.meta.raft.processor.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;

public class RetainedMsgClientTest {


    @Mock
    private Message testMsg = new Message();
    String firstTopic = "test-f1";

    String originTopic = "test-f1/f2/";
    final String groupId = Constants.RETAINEDMSG + "-" + 0;
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

        final WriteRequest request = WriteRequest.newBuilder().setGroup("retainedMsg-0").setData(ByteString.copyFrom(JSON.toJSONBytes(testMsg, SerializerFeature.WriteClassName))).putAllExtData(option).build();

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    if (err == null) {
                        future.complete(true);
                    } else {
                        future.complete(false);
                    }
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
            Assert.assertEquals(result, false);
        }));

    }

    @Test
    public void TestGetRetainedMsg() {

        HashMap<String, String> option = new HashMap<>();
        option.put("flag", "topic");
        option.put("topic", firstTopic + "/t1/");

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedmsg-0").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        CompletableFuture<Message> future = new CompletableFuture<>();

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    if (err == null) {
                        Response rsp = (Response) result;
                        if (rsp.getData().toStringUtf8().equals("null")) {
                            return;
                        }
                        String strMsg = (String) JSON.parse(rsp.getData().toStringUtf8());
                        Message message = JSON.parseObject(strMsg, Message.class);
                        future.complete(message);
                    } else {
                        future.complete(null);
                    }
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
            Mockito.verify(message, null);
        }));


    }

    @Test
    public void TestGetRetainedFromTopicTrie() {
        //test get RetainedTopicTrie
        CompletableFuture<ArrayList<String>> future = new CompletableFuture<>();

        HashMap<String, String> option = new HashMap<>();

        option.put("firstTopic", TopicUtils.normalizeTopic(firstTopic));
        option.put("topic", TopicUtils.normalizeTopic(originTopic));

        final ReadRequest request = ReadRequest.newBuilder().setGroup("retainedMsg-0").setOperation("trie").setType(Constants.READ_INDEX_TYPE).putAllExtData(option).build();

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
                @Override
                public void complete(Object result, Throwable err) {
                    if (err == null) {
                        Response rsp = (Response) result;
                        if (!rsp.getSuccess()) {
                            future.complete(null);
                            return;
                        }
                        byte[] bytes = rsp.getData().toByteArray();
                        ArrayList<String> resultList = JSON.parseObject(new String(bytes), ArrayList.class);
                        for (int i = 0; i < resultList.size(); i++) {
                            resultList.set(i, new String(Base64.getDecoder().decode(resultList.get(i))));
                        }

                        future.complete(resultList);

                    } else {
                        future.complete(null);
                    }
                }

                @Override
                public Executor executor() {
                    return null;
                }
            }, 5000);
        } catch (InterruptedException | RemotingException e) {
            throw new RuntimeException(e);
        }

        future.whenComplete(((trie, throwable) -> {
            Mockito.verify(trie, null);
        }));

    }

}
