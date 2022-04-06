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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.util.NamespaceUtil;
import org.apache.rocketmq.mqtt.common.util.StatUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.mq.MqFactory;
import org.apache.rocketmq.mqtt.exporter.collector.MqttMetricsCollector;
import org.apache.rocketmq.mqtt.exporter.exception.PrometheusException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class LmqQueueStoreManager implements LmqQueueStore {
    private static Logger logger = LoggerFactory.getLogger(LmqQueueStoreManager.class);
    private PullAPIWrapper pullAPIWrapper;
    private DefaultMQPullConsumer defaultMQPullConsumer;
    private DefaultMQProducer defaultMQProducer;
    private String consumerGroup = MixAll.CID_RMQ_SYS_PREFIX + "LMQ_PULL";
    private Map<String, Set<String>> topic2Brokers = new ConcurrentHashMap<>();

    @Resource
    private ServiceConf serviceConf;

    @Resource
    private FirstTopicManager firstTopicManager;

    @PostConstruct
    public void init() throws MQClientException {
        defaultMQPullConsumer = MqFactory.buildDefaultMQPullConsumer(consumerGroup, serviceConf.getProperties());
        defaultMQPullConsumer.setConsumerPullTimeoutMillis(2000);
        defaultMQPullConsumer.start();
        pullAPIWrapper = defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper();

        defaultMQProducer = MqFactory.buildDefaultMQProducer("GID_LMQ_SEND", serviceConf.getProperties());
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(0);
        defaultMQProducer.start();
    }

    private org.apache.rocketmq.common.message.Message toMQMessage(Message finalMessage) {
        Message message = finalMessage.copy();
        org.apache.rocketmq.common.message.Message mqMessage = new org.apache.rocketmq.common.message.Message(finalMessage.getFirstTopic(), message.getPayload());
        MessageAccessor.putProperty(mqMessage, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, message.getMsgId());
        mqMessage.putUserProperty(Constants.PROPERTY_ORIGIN_MQTT_TOPIC, message.getOriginTopic());
        if (message.getUserProperty(Message.extPropertyQoS) != null) {
            mqMessage.putUserProperty(Constants.PROPERTY_MQTT_QOS, message.getUserProperty(Message.extPropertyQoS));
        }
        if (message.getUserProperty(Message.extPropertyCleanSessionFlag) != null) {
            mqMessage.putUserProperty(Constants.PROPERTY_MQTT_CLEAN_SESSION, message.getUserProperty(Message.extPropertyCleanSessionFlag));
        }
        if (message.getUserProperty(Message.extPropertyClientId) != null) {
            mqMessage.putUserProperty(Constants.PROPERTY_MQTT_CLIENT, NamespaceUtil.decodeOriginResource(message.getUserProperty(Message.extPropertyClientId)));
            message.clearUserProperty(Message.extPropertyClientId);
        }
        mqMessage.putUserProperty(Constants.PROPERTY_MQTT_RETRY_TIMES, String.valueOf(message.getRetry()));
        Map<String, String> userProps = message.getUserProperties();
        if (userProps != null && !userProps.isEmpty()) {
            mqMessage.putUserProperty(Constants.PROPERTY_MQTT_EXT_DATA, JSONObject.toJSONString(userProps));
        }
        return mqMessage;
    }

    private Message toLmqMessage(Queue queue, MessageExt mqMessage) {
        Message message = new Message();
        message.setMsgId(mqMessage.getMsgId());
        message.setOffset(parseLmqOffset(queue, mqMessage));
        if (StringUtils.isNotBlank(mqMessage.getUserProperty(Constants.PROPERTY_ORIGIN_MQTT_TOPIC))) {
            message.setOriginTopic(mqMessage.getUserProperty(Constants.PROPERTY_ORIGIN_MQTT_TOPIC));
        } else if (StringUtils.isNotBlank(message.getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
            // maybe from rmq
            String s = message.getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
            String[] lmqSet = s.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            for (String lmq : lmqSet) {
                if (TopicUtils.isWildCard(lmq)) {
                    continue;
                }
                String originQueue = lmq.replace(MixAll.LMQ_PREFIX, "");
                message.setOriginTopic(StringUtils.replace(originQueue, "%","/"));
            }
        }
        message.setFirstTopic(mqMessage.getTopic());
        message.setPayload(mqMessage.getBody());
        message.setStoreTimestamp(mqMessage.getStoreTimestamp());
        message.setBornTimestamp(mqMessage.getBornTimestamp());
        if (StringUtils.isNotBlank(mqMessage.getUserProperty(Constants.PROPERTY_MQTT_RETRY_TIMES))) {
            message.setRetry(Integer.parseInt(mqMessage.getUserProperty(Constants.PROPERTY_MQTT_RETRY_TIMES)));
        }
        if (StringUtils.isNotBlank(mqMessage.getUserProperty(Constants.PROPERTY_MQTT_EXT_DATA))) {
            message.getUserProperties().putAll(
                    JSONObject.parseObject(mqMessage.getUserProperty(Constants.PROPERTY_MQTT_EXT_DATA),
                            new TypeReference<Map<String, String>>() {
                            }));
        }
        return message;
    }

    private long parseLmqOffset(Queue queue, MessageExt mqMessage) {
        String multiDispatchQueue = mqMessage.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return mqMessage.getQueueOffset();
        }
        String multiQueueOffset = mqMessage.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiQueueOffset)) {
            return mqMessage.getQueueOffset();
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        for (int i = 0; i < queues.length; i++) {
            if ((MixAll.LMQ_PREFIX + StringUtils.replace(queue.getQueueName(), "/","%")).equals(queues[i])) {
                return Long.parseLong(queueOffsets[i]);
            }
        }
        return mqMessage.getQueueOffset();
    }

    @Override
    public CompletableFuture<StoreResult> putMessage(Set<String> queues, Message message) {
        CompletableFuture<StoreResult> result = new CompletableFuture<>();
        org.apache.rocketmq.common.message.Message mqMessage = toMQMessage(message);
        mqMessage.setTags(Constants.MQTT_TAG);
        mqMessage.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH,
                StringUtils.join(
                        queues.stream().map(s -> MixAll.LMQ_PREFIX + StringUtils.replace(s, "/", "%")).collect(Collectors.toSet()),
                        MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        try {
            long start = System.currentTimeMillis();
            defaultMQProducer.send(mqMessage,
                    new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            result.complete(toStoreResult(sendResult));
                            long rt = System.currentTimeMillis() - start;
                            StatUtil.addInvoke("lmqWrite", rt);
                            collectLmqReadWriteMatchActionRt("lmqWrite", rt, true);
                        }

                        @Override
                        public void onException(Throwable e) {
                            logger.error("", e);
                            result.completeExceptionally(e);
                            long rt = System.currentTimeMillis() - start;
                            StatUtil.addInvoke("lmqWrite", rt, false);
                            collectLmqReadWriteMatchActionRt("lmqWrite", rt, false);
                        }
                    });
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private void collectLmqReadWriteMatchActionRt(String action, long rt, boolean status) {
        try {
            MqttMetricsCollector.collectLmqReadWriteMatchActionRt(rt, action, String.valueOf(status));
        } catch (PrometheusException e) {
            logger.error("", e);
        }
    }

    @Override
    public CompletableFuture<PullResult> pullMessage(String firstTopic, Queue queue, QueueOffset queueOffset, long count) {
        CompletableFuture<PullResult> result = new CompletableFuture<>();
        try {
            MessageQueue messageQueue = new MessageQueue(firstTopic, queue.getBrokerName(), (int) queue.getQueueId());
            long start = System.currentTimeMillis();
            String lmqTopic = MixAll.LMQ_PREFIX + StringUtils.replace(queue.getQueueName(), "/","%");
            pull(lmqTopic, messageQueue, queueOffset.getOffset(), (int) count, new PullCallback() {
                @Override
                public void onSuccess(org.apache.rocketmq.client.consumer.PullResult pullResult) {
                    result.complete(toLmqPullResult(queue, pullResult));
                    long rt = System.currentTimeMillis() - start;
                    StatUtil.addInvoke("lmqPull", rt);
                    collectLmqReadWriteMatchActionRt("lmqPull", rt, true);
                    StatUtil.addPv(pullResult.getPullStatus().name(), 1);
                    try {
                        MqttMetricsCollector.collectPullStatusTps(1, pullResult.getPullStatus().name());
                    } catch (Throwable e) {
                        logger.error("collect prometheus error", e);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    logger.error("", e);
                    result.completeExceptionally(e);
                    long rt = System.currentTimeMillis() - start;
                    StatUtil.addInvoke("lmqPull", rt, false);
                    collectLmqReadWriteMatchActionRt("lmqPull", rt, false);
                }
            });
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public CompletableFuture<PullResult> pullLastMessages(String firstTopic, Queue queue, long count) {
        CompletableFuture<Long> maxResult = queryQueueMaxOffset(queue);
        return maxResult.thenCompose(maxId -> {
            long begin = maxId - count;
            if (begin < 0) {
                begin = 0;
            }
            QueueOffset queueOffset = new QueueOffset();
            queueOffset.setOffset(begin);
            return pullMessage(firstTopic, queue, queueOffset, count);
        });
    }

    @Override
    public CompletableFuture<Long> queryQueueMaxOffset(Queue queue) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return maxOffset(queue);
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Set<String> getReadableBrokers(String firstTopic) {
        return firstTopicManager.getReadableBrokers(firstTopic);
    }

    @Override
    public String getClientRetryTopic() {
        return serviceConf.getClientRetryTopic();
    }

    @Override
    public String getClientP2pTopic() {
        return serviceConf.getClientP2pTopic();
    }


    private StoreResult toStoreResult(SendResult sendResult) {
        StoreResult storeResult = new StoreResult();
        Queue queue = new Queue();
        queue.setQueueId(sendResult.getMessageQueue().getQueueId());
        queue.setBrokerName(sendResult.getMessageQueue().getBrokerName());
        storeResult.setQueue(queue);
        storeResult.setMsgId(sendResult.getMsgId());
        return storeResult;
    }

    private PullResult toLmqPullResult(Queue queue, org.apache.rocketmq.client.consumer.PullResult pullResult) {
        PullResult lmqPullResult = new PullResult();
        if (PullStatus.OFFSET_ILLEGAL.equals(pullResult.getPullStatus())) {
            lmqPullResult.setCode(PullResult.PULL_OFFSET_MOVED);
            QueueOffset nextQueueOffset = new QueueOffset();
            nextQueueOffset.setOffset(pullResult.getNextBeginOffset());
            lmqPullResult.setNextQueueOffset(nextQueueOffset);
        } else {
            lmqPullResult.setCode(PullResult.PULL_SUCCESS);
        }
        List<MessageExt> messageExtList = pullResult.getMsgFoundList();
        if (messageExtList != null && !messageExtList.isEmpty()) {
            List<Message> messageList = new ArrayList<>(messageExtList.size());
            for (MessageExt messageExt : messageExtList) {
                Message lmqMessage = toLmqMessage(queue, messageExt);
                messageList.add(lmqMessage);
            }
            lmqPullResult.setMessageList(messageList);
        }
        return lmqPullResult;
    }

    private void pull(String lmqTopic, MessageQueue mq, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        try {
            int sysFlag = PullSysFlag.buildSysFlag(false, false, true, false);
            long timeoutMillis = 3000L;
            pullKernelImpl(
                    lmqTopic,
                    mq,
                    "*",
                    "TAG",
                    0L,
                    offset,
                    maxNums,
                    sysFlag,
                    0,
                    5000L,
                    timeoutMillis,
                    CommunicationMode.ASYNC,
                    new PullCallback() {
                        @Override
                        public void onSuccess(org.apache.rocketmq.client.consumer.PullResult pullResult) {
                            org.apache.rocketmq.client.consumer.PullResult userPullResult = pullAPIWrapper.processPullResult(mq, pullResult, new SubscriptionData());
                            pullCallback.onSuccess(userPullResult);
                        }

                        @Override
                        public void onException(Throwable e) {
                            pullCallback.onException(e);
                        }
                    });
        } catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }

    public org.apache.rocketmq.client.consumer.PullResult pullKernelImpl(
            final String lmqTopic,
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis,
            final CommunicationMode communicationMode,
            final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        MQClientInstance mQClientFactory = defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory();
        FindBrokerResult findBrokerResult =
                mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        pullAPIWrapper.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            pullAPIWrapper.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                        && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                            + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(lmqTopic);
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            String brokerAddr = findBrokerResult.getBrokerAddr();
            org.apache.rocketmq.client.consumer.PullResult pullResult =
                    mQClientFactory.getMQClientAPIImpl().pullMessage(
                            brokerAddr,
                            requestHeader,
                            timeoutMillis,
                            communicationMode,
                            pullCallback);

            return pullResult;
        }
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    private long maxOffset(Queue queue) throws MQClientException {
        String lmqTopic = MixAll.LMQ_PREFIX + StringUtils.replace(queue.getQueueName(), "/","%");
        MQClientInstance mQClientFactory = defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory();
        String brokerAddr = mQClientFactory.findBrokerAddressInPublish(queue.getBrokerName());
        if (null == brokerAddr) {
            mQClientFactory.updateTopicRouteInfoFromNameServer(queue.toFirstTopic());
            brokerAddr = mQClientFactory.findBrokerAddressInPublish(queue.getBrokerName());
        }

        if (brokerAddr != null) {
            try {
                return mQClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, lmqTopic, (int) queue.getQueueId(), 3000L);
            } catch (Exception e) {
                throw new MQClientException("Invoke Broker[" + brokerAddr + "] exception", e);
            }
        }

        throw new MQClientException("The broker[" + queue.getBrokerName() + "] not exist", null);
    }
}
