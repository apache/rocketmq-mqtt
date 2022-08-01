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
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.common.util.TrieUtil;
import org.apache.rocketmq.mqtt.ds.retain.RetainedMsgClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import java.util.Map;
import java.util.Set;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class RetainedPersistManagerImpl implements RetainedPersistManager {

    private static Logger logger = LoggerFactory.getLogger(RetainedPersistManagerImpl.class);

    private volatile Map<String, Trie<String, String>> localRetainedTopicTrieCache = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor refreshScheduler;  //ThreadPool to refresh local Trie

    @Resource
    private MetaPersistManager metaPersistManager;

    public void init() {
        refreshScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("refreshKVStore"));
        refreshScheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshKVStore();
            } catch (Throwable t) {
                logger.error("", t);
            }
        }, 3, 3, TimeUnit.SECONDS);

//        deleteTopicTrieScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("deleteTopicKvStore"));
//        deleteTopicTrieScheduler.scheduleWithFixedDelay(() -> {
//            try {
//                refreshDeleteQueue();
//            } catch (Throwable t) {
//                logger.error("", t);
//            }
//        }, 5, 3, TimeUnit.SECONDS);
    }


    @Override
    public Trie<String, String> getTries(String firstTopic) {
        Set<String> allFirstTopics = metaPersistManager.getAllFirstTopics();
        if (!allFirstTopics.contains(firstTopic)) {
            return null;
        } else {
            return localRetainedTopicTrieCache.get(firstTopic);
        }
    }

    private void refreshKVStore() throws RemotingException, InterruptedException { //Refresh all firstTopic
        long start = System.currentTimeMillis();
        logger.info(" Start refresh the tries...");
        Set<String> allFirstTopics = metaPersistManager.getAllFirstTopics();
        for (String firstTopic : allFirstTopics) {

            //If local Trie is null
            if (localRetainedTopicTrieCache.get(TopicUtils.normalizeTopic(firstTopic)) == null) {
                localRetainedTopicTrieCache.put(TopicUtils.normalizeTopic(firstTopic), new Trie<String, String>());
            }
            refreshSingleTopicTrieStore(TopicUtils.normalizeTopic(firstTopic));

            logger.info("firstTopic:" + TopicUtils.normalizeTopic(firstTopic));
            logger.info("firstTopic Trie: " + localRetainedTopicTrieCache.get(TopicUtils.normalizeTopic(firstTopic)).toString());

        }
        logger.info("Refresh the tries cost rt {}", System.currentTimeMillis() - start);
    }

    private void refreshSingleTopicTrieStore(String firstTopic) throws RemotingException, InterruptedException {   //Refresh single firstTopic

        CompletableFuture<Trie<String, String>> completableFuture = new CompletableFuture<>();

        RetainedMsgClient.GetRetainedTopicTrie(TopicUtils.normalizeTopic(firstTopic), completableFuture);

        completableFuture.whenComplete((tmpTrie, throwable) -> {

            Trie<String, String> kvTrie = null;
            kvTrie = tmpTrie;

            logger.info("The firstTopic {} kvTrie: {}", firstTopic, kvTrie.toString());
            Trie<String, String> localTrie = TrieUtil.rebuildLocalTrie(kvTrie);
            TrieUtil.mergeKvToLocal(localTrie, kvTrie);         //local<-kvTrie
            localRetainedTopicTrieCache.put(firstTopic, localTrie);

        });

    }

    public CompletableFuture<Boolean> storeRetainedMessage(String topic, Message message) throws InterruptedException, RemotingException {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        if (!metaPersistManager.getAllFirstTopics().contains(message.getFirstTopic())) {
            logger.info("Put retained message of topic {} into meta failed. Because first topic {} does not exist...", topic, message.getFirstTopic());
            result.complete(false);
            return result;
        }
        logger.info("Start store retain msg...");
        RetainedMsgClient.SetRetainedMsg(topic, message, result);

        return result;

    }

    public CompletableFuture<Message> getRetainedMessage(String preciseTopic) throws InterruptedException, RemotingException {  //precise preciseTopic
        CompletableFuture<Message> future = new CompletableFuture<>();
        RetainedMsgClient.GetRetainedMsg(preciseTopic, future);
        return future;
    }

    public Set<String> getTopicsFromTrie(Subscription subscription) throws RemotingException, InterruptedException {
        Set<String> results;
        String firstTopic = subscription.toFirstTopic();
        String originTopicFilter = subscription.getTopicFilter();
        logger.info("firstTopic={} originTopicFilter={}", firstTopic, originTopicFilter);
        results = localRetainedTopicTrieCache.get(firstTopic).getAllPath(originTopicFilter);
        if (results.isEmpty()) {   //Refresh the trie about single firstTopic
            logger.info("Local trie does not exist. Try to find...");
            refreshSingleTopicTrieStore(firstTopic);
            results = localRetainedTopicTrieCache.get(firstTopic).getAllPath(originTopicFilter);
        }
        return results;
    }


}
