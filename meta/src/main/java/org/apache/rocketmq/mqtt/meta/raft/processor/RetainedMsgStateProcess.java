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
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.SnapshotOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public class RetainedMsgStateProcess extends StateProcessor {
    @Resource
    private MetaConf metaConf;
    private static Logger logger = LoggerFactory.getLogger(RetainedMsgStateProcess.class);

    private final AtomicLong value = new AtomicLong(0);
    private final ConcurrentHashMap<String, String> retainedMsgMap = new ConcurrentHashMap<>();  //key:topic value:retained msg
    private final ConcurrentHashMap<String, Trie<String, String>> retainedMsgTopicTrie = new ConcurrentHashMap<>();  //key:firstTopic value:retained topic Trie

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private SnapshotOperation snapshotOperation;


    @Override
    public Response onReadRequest(ReadRequest request) {
        try {
            String topic = request.getExtDataMap().get("topic");
            String firstTopic = request.getExtDataMap().get("firstTopic");
            String operation = request.getOperation();

            logger.info("FirstTopic:{} Topic:{} Operation:{}",firstTopic,topic,operation);

            if (operation.equals("topic")) {    //return retained msg
                String msg = retainedMsgMap.get(topic);
                return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(JSON.toJSONBytes(msg)))
                    .build();
            } else { //return retain msgs of matched Topic
                if (!retainedMsgTopicTrie.containsKey(firstTopic)) {
                    Trie<String, String> newTrie = new Trie<>();
                    retainedMsgTopicTrie.put(firstTopic, newTrie);
                }
                Trie<String, String> tmpTrie = retainedMsgTopicTrie.get(firstTopic);

                Set<String> matchTopics = tmpTrie.getAllPath(topic);

                ArrayList<String> msgResults = new ArrayList<>();

                for (String tmpTopic:matchTopics) {
                    String msg = retainedMsgMap.get(tmpTopic);
                    if (msg != null) {
                        msgResults.add(msg);
                    }
                }

                return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(JSON.toJSONBytes(msgResults)))   //return retained msgs of matched Topic
                    .build();
            }
        } catch (Exception e) {
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }
    }

    boolean setRetainedMsg(String firstTopic,String topic, boolean isEmpty,String msg) {

        // if the trie of firstTopic doesn't exist
        if (!retainedMsgTopicTrie.containsKey(firstTopic)) {
            retainedMsgTopicTrie.put(TopicUtils.normalizeTopic(firstTopic), new Trie<String, String>());
        }

        if (isEmpty) {
            //delete from trie
            logger.info("Delete the topic {} retained message", topic);
            retainedMsgMap.remove(TopicUtils.normalizeTopic(topic));
            retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(firstTopic)).deleteTrieNode(topic, "");
        } else {
            //Add to trie
            Trie<String, String> trie = retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(firstTopic));
            if (trie.getNodePath().size() < metaConf.getLimitRetainedMessageCount()) {
                retainedMsgMap.put(TopicUtils.normalizeTopic(topic), msg);
                retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(firstTopic)).addNode(topic, "", "");
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
            String firstTopic = TopicUtils.normalizeTopic(writeRequest.getExtDataMap().get("firstTopic"));     //retained msg firstTopic
            String topic = TopicUtils.normalizeTopic(writeRequest.getExtDataMap().get("topic"));     //retained msg topic
            boolean isEmpty = Boolean.parseBoolean(writeRequest.getExtDataMap().get("isEmpty"));     //retained msg is empty
            String message = writeRequest.getExtDataMap().get("message");  //retained msg

            boolean res = setRetainedMsg(firstTopic,topic, isEmpty,message);
            if (!res) {
                logger.warn("Put the topic {} retained message failed! Exceeded maximum number of reserved topics limit.",topic);
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
            logger.error("Put the retained message error! {}",e.getMessage());
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }


    }

    @Override
    public SnapshotOperation loadSnapshotOperate() {
        return snapshotOperation;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally) {

    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return true;
    }

    @Override
    public String groupCategory() {
        return Constants.RETAINEDMSG;
    }

}
