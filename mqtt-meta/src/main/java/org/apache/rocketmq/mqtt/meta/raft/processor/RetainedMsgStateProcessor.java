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

package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;
import org.apache.rocketmq.mqtt.meta.raft.MqttStateMachine;
import org.apache.rocketmq.mqtt.common.meta.Constants;
import org.apache.rocketmq.mqtt.meta.rocksdb.RocksDBEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class RetainedMsgStateProcessor extends StateProcessor {
    private static Logger logger = LoggerFactory.getLogger(RetainedMsgStateProcessor.class);
    private final ConcurrentHashMap<String, Trie<String, String>> retainedMsgTopicTrie = new ConcurrentHashMap<>();  //key:firstTopic value:retained topic Trie
    private MqttRaftServer server;
    private int maxRetainedMessageNum;

    public RetainedMsgStateProcessor(MqttRaftServer server, int maxRetainedMessageNum) {
        this.server = server;
        this.maxRetainedMessageNum = maxRetainedMessageNum;
    }

    @Override
    public Response onReadRequest(ReadRequest request) {
        try {
            MqttStateMachine sm = server.getMqttStateMachine(request.getGroup());
            if (sm == null) {
                logger.error("Fail to process RetainedMsg ReadRequest , Not Found SM for {}", request.getGroup());
                return null;
            }
            String topic = request.getExtDataMap().get("topic");
            String firstTopic = request.getExtDataMap().get("firstTopic");
            String operation = request.getOperation();

            logger.info("FirstTopic:{} Topic:{} Operation:{}", firstTopic, topic, operation);

            if (operation.equals("topic")) {    //return retained msg
                return get(sm.getRocksDBEngine(), topic.getBytes(StandardCharsets.UTF_8));
            } else { //return retain msgs of matched Topic
                if (!retainedMsgTopicTrie.containsKey(firstTopic)) {
                    Trie<String, String> newTrie = new Trie<>();
                    Response value = get(sm.getRocksDBEngine(), firstTopic.getBytes(StandardCharsets.UTF_8));
                    if (value != null && value.getData() != null) {
                        newTrie = JSON.parseObject(value.getData().toStringUtf8(), Trie.class);
                    }
                    retainedMsgTopicTrie.put(firstTopic, newTrie);

                    return Response.newBuilder()
                            .setSuccess(true)
                            .setData(ByteString.copyFrom(JSON.toJSONBytes(new ArrayList<byte[]>())))
                            .build();
                }
                Trie<String, String> tmpTrie = retainedMsgTopicTrie.get(firstTopic);
                Set<String> matchTopics = tmpTrie.getAllPath(topic);

                ArrayList<ByteString> msgResults = new ArrayList<>();

                for (String tmpTopic : matchTopics) {
                    Response value = get(sm.getRocksDBEngine(), tmpTopic.getBytes(StandardCharsets.UTF_8));
                    if (value != null && value.getData() != null) {
                        msgResults.add(ByteString.copyFrom(value.getData().toByteArray()));
                    }
                }
                return Response.newBuilder()
                        .setSuccess(true)
                        .addAllDatalist(msgResults)//return retained msgs of matched Topic
                        .build();
            }
        } catch (Exception e) {
            logger.error("", e);
            return Response.newBuilder()
                    .setSuccess(false)
                    .setErrMsg(e.getMessage())
                    .build();
        }
    }

    boolean setRetainedMsg(RocksDBEngine rocksDBEngine, String firstTopic, String topic, boolean isEmpty, byte[] msg) throws Exception {

        // if the trie of firstTopic doesn't exist
        if (!retainedMsgTopicTrie.containsKey(firstTopic)) {
            retainedMsgTopicTrie.put(TopicUtils.normalizeTopic(firstTopic), new Trie<String, String>());
        }

        if (isEmpty) {
            //delete from trie
            logger.info("Delete the topic {} retained message", topic);
            delete(rocksDBEngine, topic.getBytes(StandardCharsets.UTF_8));
            Trie<String, String> trie = retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(firstTopic));
            if (trie != null) {
                trie.deleteTrieNode(topic, "");
            }
            put(rocksDBEngine, firstTopic.getBytes(StandardCharsets.UTF_8), JSON.toJSONBytes(trie));
        } else {
            //Add to trie
            Trie<String, String> trie = retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(firstTopic));
            logger.info("maxRetainedMessageNum:{}", maxRetainedMessageNum);
            if (trie.getNodePath().size() < maxRetainedMessageNum) {
                put(rocksDBEngine, topic.getBytes(StandardCharsets.UTF_8), msg);
                trie.addNode(topic, "", "");
                put(rocksDBEngine, firstTopic.getBytes(StandardCharsets.UTF_8), JSON.toJSONBytes(trie));
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    @Override
    public Response onWriteRequest(WriteRequest writeRequest) {
        try {
            MqttStateMachine sm = server.getMqttStateMachine(writeRequest.getGroup());
            if (sm == null) {
                logger.error("Fail to process RetainedMsg WriteRequest , Not Found SM for {}", writeRequest.getGroup());
                return null;
            }
            String firstTopic = TopicUtils.normalizeTopic(writeRequest.getExtDataMap().get("firstTopic"));     //retained msg firstTopic
            String topic = TopicUtils.normalizeTopic(writeRequest.getExtDataMap().get("topic"));     //retained msg topic
            boolean isEmpty = Boolean.parseBoolean(writeRequest.getExtDataMap().get("isEmpty"));     //retained msg is empty
            byte[] message = writeRequest.getData().toByteArray();
            boolean res = setRetainedMsg(sm.getRocksDBEngine(), firstTopic, topic, isEmpty, message);
            if (!res) {
                logger.warn("Put the topic {} retained message failed! Exceeded maximum number of reserved topics limit.", topic);
                return Response.newBuilder()
                        .setSuccess(false)
                        .setErrMsg("Exceeded maximum number of reserved topics limit.")
                        .build();
            }
            logger.info("Put the topic {} retained message success!", topic);

            return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(JSON.toJSONBytes(topic)))
                    .build();
        } catch (Exception e) {
            logger.error("Put the retained message error!", e);
            return Response.newBuilder()
                    .setSuccess(false)
                    .setErrMsg(e.getMessage())
                    .build();
        }
    }

    @Override
    public String groupCategory() {
        return Constants.CATEGORY_RETAINED_MSG;
    }

}
