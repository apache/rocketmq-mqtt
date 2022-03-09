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

package org.apache.rocketmq.mqtt.ds.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.mqtt.common.facade.LmqOffsetStore;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.mq.MqFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class LmqOffsetStoreManager implements LmqOffsetStore {
    private static Logger logger = LoggerFactory.getLogger(LmqOffsetStoreManager.class);
    private DefaultMQPullConsumer defaultMQPullConsumer;

    @Resource
    private ServiceConf serviceConf;

    @Resource
    private FirstTopicManager firstTopicManager;

    @PostConstruct
    public void init() throws MQClientException {
        defaultMQPullConsumer = MqFactory.buildDefaultMQPullConsumer(MixAll.CID_RMQ_SYS_PREFIX + "LMQ_OFFSET" +
                "", serviceConf.getProperties());
        defaultMQPullConsumer.setConsumerPullTimeoutMillis(2000);
        defaultMQPullConsumer.start();
    }

    @Override
    public void save(String clientId, Map<Subscription, Map<Queue, QueueOffset>> offsetMap) {
        if (offsetMap == null || offsetMap.isEmpty()) {
            return;
        }
        for (Map.Entry<Subscription, Map<Queue, QueueOffset>> entry : offsetMap.entrySet()) {
            Map<String, String> tmpBrokerAddressMap = findBrokers(entry.getKey());
            if (tmpBrokerAddressMap == null || tmpBrokerAddressMap.isEmpty()) {
                continue;
            }
            for (Map.Entry<Queue, QueueOffset> each : entry.getValue().entrySet()) {
                try {
                    Queue queue = each.getKey();
                    if (StringUtils.isBlank(queue.getBrokerName())) {
                        continue;
                    }
                    String brokerAddress = tmpBrokerAddressMap.get(queue.getBrokerName());
                    QueueOffset queueOffset = each.getValue();
                    UpdateConsumerOffsetRequestHeader updateHeader = new UpdateConsumerOffsetRequestHeader();
                    updateHeader.setTopic(queue.getQueueName());
                    updateHeader.setConsumerGroup(LmqQueueStore.LMQ_PREFIX + clientId);
                    updateHeader.setQueueId((int) queue.getQueueId());
                    updateHeader.setCommitOffset(queueOffset.getOffset());
                    defaultMQPullConsumer
                            .getDefaultMQPullConsumerImpl()
                            .getRebalanceImpl()
                            .getmQClientFactory()
                            .getMQClientAPIImpl().updateConsumerOffset(brokerAddress, updateHeader, 1000);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Map<Queue, QueueOffset>> getOffset(String clientId, Subscription subscription) {
        return CompletableFuture.supplyAsync(() -> {
            Map<Queue, QueueOffset> map = new HashMap<>();
            Map<String, String> tmpBrokerAddressMap = findBrokers(subscription);
            if (tmpBrokerAddressMap == null || tmpBrokerAddressMap.isEmpty()) {
                return map;
            }
            for (Map.Entry<String, String> entry : tmpBrokerAddressMap.entrySet()) {
                Queue queue = new Queue(0, subscription.toQueueName(), entry.getKey());
                String brokerAddress = entry.getValue();
                QueueOffset queueOffset = new QueueOffset();
                map.put(queue, queueOffset);
                try {
                    QueryConsumerOffsetRequestHeader queryHeader = new QueryConsumerOffsetRequestHeader();
                    queryHeader.setTopic(queue.getQueueName());
                    queryHeader.setConsumerGroup(LmqQueueStore.LMQ_PREFIX + clientId);
                    queryHeader.setQueueId((int) queue.getQueueId());
                    long offset = defaultMQPullConsumer
                            .getDefaultMQPullConsumerImpl()
                            .getRebalanceImpl()
                            .getmQClientFactory()
                            .getMQClientAPIImpl()
                            .queryConsumerOffset(brokerAddress, queryHeader, 1000);
                    queueOffset.setOffset(offset);
                } catch (MQBrokerException e) {
                    if (ResponseCode.QUERY_NOT_FOUND == e.getResponseCode()) {
                        queueOffset.setOffset(Long.MAX_VALUE);
                    }
                } catch (Exception e) {
                    logger.error("{}", clientId, e);
                    throw new RuntimeException(e);
                }
            }
            return map;
        });
    }

    private Map<String, String> findBrokers(Subscription subscription) {
        String firstTopic = subscription.toFirstTopic();
        if (subscription.isRetry()) {
            firstTopic = serviceConf.getClientRetryTopic();
        }
        if (subscription.isP2p()) {
            if (StringUtils.isNotBlank(serviceConf.getClientP2pTopic())) {
                firstTopic = serviceConf.getClientP2pTopic();
            } else {
                firstTopic = serviceConf.getClientRetryTopic();
            }
        }
        return firstTopicManager.getBrokerAddressMap(firstTopic);
    }

}
