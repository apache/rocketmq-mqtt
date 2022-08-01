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
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.SnapshotOperation;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.impl.CounterSnapshotOperation;

import java.util.HashMap;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public class RetainedMsgStateProcess extends StateProcessor {

    private final AtomicLong value = new AtomicLong(0);

    private final HashMap<String, String> retainedMsgMap = new HashMap<>();  //key:topic value:retained msg

    private final HashMap<String, Trie<String, String>> retainedMsgTopicTrie = new HashMap<>();  //key:firstTopic value:retained topic Trie

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int LIMIT_RETAINED_MESSAGE_COUNT = 100;
    private SnapshotOperation snapshotOperation;


    @Override
    public Response onReadRequest(ReadRequest request) {
        try {
            String topic = request.getExtDataMap().get("topic");
            String flag = request.getExtDataMap().get("flag");

            if (flag.equals("topic")) {    //return retained msg
                String msg = retainedMsgMap.get(topic);
                return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(JSON.toJSONBytes(msg)))
                    .build();
            } else {
                if (!retainedMsgTopicTrie.containsKey(topic)) {
                    Trie<String, String> newTrie = new Trie<>();
                    retainedMsgTopicTrie.put(topic, newTrie);
                }
                Trie<String, String> tmpTrie = retainedMsgTopicTrie.get(topic);     //return firstTopic trie

                return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(JSON.toJSONBytes(tmpTrie)))
                    .build();
            }
        } catch (Exception e) {
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }
    }

    boolean setRetainedMsg(String topic, String msg) {


        // if message is empty
        Message message = JSON.parseObject(msg, Message.class);

        if (!retainedMsgTopicTrie.containsKey(message.getFirstTopic())) {
            retainedMsgTopicTrie.put(TopicUtils.normalizeTopic(message.getFirstTopic()), new Trie<String, String>());
        }

        if (message.isEmpty()) {
            //delete from trie
            retainedMsgMap.put(TopicUtils.normalizeTopic(topic), msg);
            retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(message.getFirstTopic())).deleteTrieNode(message.getOriginTopic(), "");
        } else {
            //Add to trie
            Trie<String, String> trie = retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(message.getFirstTopic()));
            if (trie.getNodePath().size() < LIMIT_RETAINED_MESSAGE_COUNT) {
                retainedMsgMap.put(TopicUtils.normalizeTopic(topic), msg);
                retainedMsgTopicTrie.get(TopicUtils.normalizeTopic(message.getFirstTopic())).addNode(message.getOriginTopic(), "", "");
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
            String topic = TopicUtils.normalizeTopic(writeRequest.getExtDataMap().get("topic"));     //retained msg topic
            String message = writeRequest.getExtDataMap().get("message");  //retained msg

            boolean res = setRetainedMsg(topic, message);
            if (!res) {
                return Response.newBuilder()
                    .setSuccess(false)
                    .setErrMsg("Exceeded maximum number of reserved topics limit.")
                    .build();
            }

            return Response.newBuilder()
                .setSuccess(true)
                .setData(ByteString.copyFrom(JSON.toJSONBytes(topic)))
                .build();
        } catch (Exception e) {
            return Response.newBuilder()
                .setSuccess(false)
                .setErrMsg(e.getMessage())
                .build();
        }


    }

    @Override
    public SnapshotOperation loadSnapshotOperate() {
        snapshotOperation = new CounterSnapshotOperation(lock);
        return snapshotOperation;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally) {
        snapshotOperation.onSnapshotSave(writer, callFinally, value.toString());
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        String load = snapshotOperation.onSnapshotLoad(reader);
        value.set(Long.parseLong(load));
        return true;
    }

    @Override
    public String groupCategory() {
        return Constants.RETAINEDMSG;
    }

}
