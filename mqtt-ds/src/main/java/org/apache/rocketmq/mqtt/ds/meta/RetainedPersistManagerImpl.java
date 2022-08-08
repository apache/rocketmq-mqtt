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

import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;

import org.apache.rocketmq.mqtt.ds.retain.RetainedMsgClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Resource;
import java.util.ArrayList;


import java.util.concurrent.CompletableFuture;


public class RetainedPersistManagerImpl implements RetainedPersistManager {

    private static Logger logger = LoggerFactory.getLogger(RetainedPersistManagerImpl.class);


    @Resource
    private MetaPersistManager metaPersistManager;

    public void init() {


    }





    public CompletableFuture<Boolean> storeRetainedMessage(String topic, Message message) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        if (!metaPersistManager.getAllFirstTopics().contains(message.getFirstTopic())) {
            logger.info("Put retained message of topic {} into meta failed. Because first topic {} does not exist...", topic, message.getFirstTopic());
            result.complete(false);
            return result;
        }
        logger.info("Start store retain msg...");

        try {
            RetainedMsgClient.SetRetainedMsg(topic, message, result);
        } catch (RemotingException | InterruptedException e) {
            logger.error("",e);
            result.completeExceptionally(e);
        }

        return result;
    }

    public CompletableFuture<Message> getRetainedMessage(String preciseTopic) {  //precise preciseTopic
        CompletableFuture<Message> future = new CompletableFuture<>();
        logger.info("topic:" + preciseTopic);
        try {
            RetainedMsgClient.GetRetainedMsg(preciseTopic, future);
        } catch (RemotingException | InterruptedException e) {
            logger.error("",e);
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<ArrayList<String>> getMsgsFromTrie(Subscription subscription) {
        String firstTopic = subscription.toFirstTopic();
        String originTopicFilter = subscription.getTopicFilter();
        logger.info("firstTopic={} originTopicFilter={}", firstTopic, originTopicFilter);

        CompletableFuture<ArrayList<String>> future = new CompletableFuture<>();
        try {
            RetainedMsgClient.GetRetainedMsgsFromTrie(firstTopic, originTopicFilter,future);
        } catch (RemotingException | InterruptedException e) {
            logger.error("",e);
            future.completeExceptionally(e);
        }

        return future;
    }


}
