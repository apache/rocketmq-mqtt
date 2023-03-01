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

import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.StoreMessage;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.CATEGORY_RETAINED_MSG;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.NOT_FOUND;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.READ_INDEX_TYPE;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_READ_PARAM_FIRST_TOPIC;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_READ_PARAM_OPERATION_TOPIC;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_READ_PARAM_OPERATION_TRIE;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_READ_PARAM_TOPIC;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_WRITE_PARAM_FIRST_TOPIC;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_WRITE_PARAM_IS_EMPTY;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_WRITE_PARAM_TOPIC;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.RETAIN_REQ_WRITE_PARAM_EXPIRE;
import static org.apache.rocketmq.mqtt.common.meta.RaftUtil.RETAIN_RAFT_GROUP_INDEX;


@Service
public class RetainedMsgClient {
    private static Logger logger = LoggerFactory.getLogger(RetainedMsgClient.class);

    @Resource
    private MetaRpcClient metaRpcClient;

    @Resource
    public ServiceConf serviceConf;

    public void setRetainedMsg(String topic, Message msg, CompletableFuture<Boolean> future) throws RemotingException, InterruptedException {
        _setRetainedMsg(topic, msg, null, future);
    }

    public void _setRetainedMsg(String topic, Message msg, Long expire, CompletableFuture<Boolean> future) throws RemotingException, InterruptedException {
        String groupId = whichGroup();
        HashMap<String, String> option = new HashMap<>();
        option.put(RETAIN_REQ_WRITE_PARAM_TOPIC, topic);
        option.put(RETAIN_REQ_WRITE_PARAM_FIRST_TOPIC, msg.getFirstTopic());
        option.put(RETAIN_REQ_WRITE_PARAM_IS_EMPTY, String.valueOf(msg.isEmpty()));
        if (expire != null) {
            option.put(RETAIN_REQ_WRITE_PARAM_EXPIRE, String.valueOf(expire));
        }

        logger.debug("SetRetainedMsg option:" + option);

        final WriteRequest request = WriteRequest.newBuilder()
                .setGroup(groupId)
                .setData(ByteString.copyFrom(msg.getEncodeBytes()))
                .putAllExtData(option)
                .setCategory(CATEGORY_RETAINED_MSG)
                .build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.error("SetRetainedMsg failed. {}", rsp.getErrMsg());
                        future.complete(false);
                        return;
                    }
                    future.complete(true);
                } else {
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

    public void GetRetainedMsgsFromTrie(String firstTopic, String topic, CompletableFuture<ArrayList<Message>> future) throws RemotingException, InterruptedException {
        String groupId = whichGroup();
        HashMap<String, String> option = new HashMap<>();

        option.put(RETAIN_REQ_READ_PARAM_FIRST_TOPIC, firstTopic);
        option.put(RETAIN_REQ_READ_PARAM_TOPIC, topic);

        logger.debug("GetRetainedMsgsFromTrie option:" + option);

        final ReadRequest request = ReadRequest.newBuilder()
                .setGroup(groupId)
                .setOperation(RETAIN_REQ_READ_PARAM_OPERATION_TRIE)
                .setType(READ_INDEX_TYPE)
                .putAllExtData(option)
                .setCategory(CATEGORY_RETAINED_MSG)
                .build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.error("GetRetainedTopicTrie failed. {}", rsp.getErrMsg());
                        future.complete(null);
                        return;
                    }
                    List<ByteString> datalistList = rsp.getDatalistList();
                    ArrayList<Message> resultList = new ArrayList<>();
                    for (ByteString tmp : datalistList) {
                        try {
                            Message message = Message.copyFromStoreMessage(StoreMessage.parseFrom(tmp.toByteArray()));
                            if (System.currentTimeMillis() - message.getBornTimestamp() > serviceConf.getRetainMsgExpire()) {
                                message.setPayload(null);
                                message.setEmpty(true);
                                try {
                                    _setRetainedMsg(topic, message, serviceConf.getRetainMsgExpire(), new CompletableFuture<>());
                                } catch (Exception e) {
                                    logger.error("", e);
                                }
                                continue;
                            }
                            resultList.add(message);
                        } catch (InvalidProtocolBufferException e) {
                            future.complete(null);
                            throw new RuntimeException(e);
                        }
                    }
                    future.complete(resultList);
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

    public void GetRetainedMsg(String topic, CompletableFuture<Message> future) throws RemotingException, InterruptedException {
        String groupId = whichGroup();
        HashMap<String, String> option = new HashMap<>();
        option.put(RETAIN_REQ_READ_PARAM_TOPIC, topic);

        final ReadRequest request = ReadRequest.newBuilder()
                .setGroup(groupId)
                .setOperation(RETAIN_REQ_READ_PARAM_OPERATION_TOPIC)
                .setType(READ_INDEX_TYPE)
                .putAllExtData(option)
                .setCategory(CATEGORY_RETAINED_MSG)
                .build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, new InvokeCallback() {

            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    Response rsp = (Response) result;
                    if (!rsp.getSuccess()) {
                        logger.info("GetRetainedMsg failed. {}", rsp.getErrMsg());
                        future.complete(null);
                        return;
                    }
                    if (rsp.getData().toStringUtf8().equals(NOT_FOUND)) {  //this topic doesn't exist retained msg
                        future.complete(null);
                        return;
                    }
                    Message message = null;
                    try {
                        message = Message.copyFromStoreMessage(StoreMessage.parseFrom(rsp.getData().toByteArray()));
                        if (System.currentTimeMillis() - message.getBornTimestamp() > serviceConf.getRetainMsgExpire()) {
                            message.setPayload(null);
                            message.setEmpty(true);
                            try {
                                _setRetainedMsg(topic, message, serviceConf.getRetainMsgExpire(), new CompletableFuture<>());
                            } catch (Exception e) {
                                logger.error("", e);
                            }
                            message = null;
                        }
                    } catch (Exception e) {
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

    private String whichGroup() {
        return metaRpcClient.getRaftGroups()[RETAIN_RAFT_GROUP_INDEX];
    }
}
