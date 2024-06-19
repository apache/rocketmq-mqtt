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

package org.apache.rocketmq.mqtt.common.facade;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.StoreResult;

public interface LmqQueueStore {

    /**
     * put message and atomic dispatch to multiple queues
     *
     * @param queues
     * @param message
     * @return
     */
    CompletableFuture<StoreResult> putMessage(Set<String> queues, Message message);

    /**
     * put client event messages to RocketMQ LMQ
     * @param queues
     * @param messageList
     * @return
     */
    CompletableFuture<StoreResult> putMessage(Set<String> queues, List<Message> messageList);

    /**
     * pull messages
     *
     * @param firstTopic
     * @param queue
     * @param queueOffset
     * @param count
     * @return
     */
    CompletableFuture<PullResult> pullMessage(String firstTopic, Queue queue, QueueOffset queueOffset, long count);

    /**
     * pop messages
     *
     * @param consumerGroup
     * @param firstTopic
     * @param queue
     * @param count
     * @return
     */
    CompletableFuture<PullResult> popMessage(String consumerGroup, String firstTopic, Queue queue, long count);

    void popAck(String lmqTopic, String consumerGroup, Message message);

    /**
     * pull last messages
     *
     * @param firstTopic
     * @param queue
     * @param count
     * @return
     */
    CompletableFuture<PullResult> pullLastMessages(String firstTopic, Queue queue, long count);

    /**
     * query maxId of Queue
     *
     * @param queue
     * @return
     */
    CompletableFuture<Long> queryQueueMaxOffset(Queue queue);

    /**
     * get readable brokers of the topic
     *
     * @param firstTopic
     * @return
     */
    Set<String> getReadableBrokers(String firstTopic);

    /**
     * retry topic of one mqtt client
     *
     * @return
     */
    String getClientRetryTopic();

    /**
     * p2p topic of one mqtt client
     *
     * @return
     */
    String getClientP2pTopic();
}
