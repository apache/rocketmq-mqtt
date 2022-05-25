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

package org.apache.rocketmq.mqtt.cs.session.infly;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Component
public class RetryDriver {
    private static Logger logger = LoggerFactory.getLogger(RetryDriver.class);

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private MqttMsgId mqttMsgId;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private PushAction pushAction;

    @Resource
    private ChannelManager channelManager;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private QueueFresh queueFresh;

    private Cache<String, RetryMessage> retryCache;
    private static final int MAX_CACHE = 50000;
    private int scheduleDelaySecs = 3;
    private long messageRetryInterval = 3000;
    private Map<String, Map<Integer, RetryMessage>> sessionNoWaitRetryMsgMap = new ConcurrentHashMap<>(16);
    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(2,
            new ThreadFactoryImpl("retry_msg_thread_"));

    @PostConstruct
    public void init() {
        retryCache = Caffeine.newBuilder().maximumSize(MAX_CACHE).removalListener((RemovalListener<String, RetryMessage>) (key, value, cause) -> {
            if (value == null || key == null) {
                return;
            }
            if (cause.wasEvicted()) {
                saveRetryQueue(key, value);
            }
        }).build();

        scheduler.scheduleWithFixedDelay(() -> doRetryCache(), scheduleDelaySecs, connectConf.getRetryIntervalSeconds(), TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Map<String, RetryMessage> map = retryCache.asMap();
            if (map == null) {
                return;
            }
            for (Map.Entry<String, RetryMessage> entry : map.entrySet()) {
                saveRetryQueue(entry.getKey(), entry.getValue());
            }
        }));
    }

    public void unloadSession(Session session) {
        if (session == null) {
            return;
        }
        Map<Integer, RetryMessage> map = sessionNoWaitRetryMsgMap.remove(session.getChannelId());
        if (map == null || map.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, RetryMessage> entry : map.entrySet()) {
            String cacheKey = buildKey(entry.getKey(), session.getChannelId());
            retryCache.invalidate(cacheKey);
            RetryMessage retryMessage = entry.getValue();
            saveRetryQueue(cacheKey, retryMessage);
        }
    }

    private void saveRetryQueue(String key, RetryMessage retryMessage) {
        Message message = retryMessage.message.copy();
        message.setFirstTopic(lmqQueueStore.getClientRetryTopic());
        Session session = retryMessage.session;
        int mqttMsgId = retryMessage.mqttMsgId;
        String clientId = session.getClientId();
        if (message.getRetry() >= connectConf.getMaxRetryTime()) {
            pushAction.rollNext(session, retryMessage.mqttMsgId);
            return;
        }
        String retryQueue = Subscription.newRetrySubscription(clientId).toQueueName();
        CompletableFuture<StoreResult> result = lmqQueueStore.putMessage(new HashSet<>(Arrays.asList(retryQueue)), message);
        result.whenComplete((storeResult, throwable) -> {
            if (throwable != null) {
                retryCache.put(key, retryMessage);
                return;
            }
            long queueId = storeResult.getQueue().getQueueId();
            String brokerName = storeResult.getQueue().getBrokerName();
            pushAction.rollNext(session, mqttMsgId);
            scheduler.schedule(() -> {
                Subscription subscription = Subscription.newRetrySubscription(clientId);
                List<Session> sessionList = sessionLoop.getSessionList(clientId);
                if (sessionList != null) {
                    for (Session eachSession : sessionList) {
                        Set<Queue> set = queueFresh.freshQueue(eachSession, subscription);
                        if (set == null || set.isEmpty()) {
                            continue;
                        }
                        for (Queue queue : set) {
                            if (Objects.equals(queue.getBrokerName(), brokerName)) {
                                sessionLoop.notifyPullMessage(eachSession, subscription, queue);
                            }
                        }
                    }
                }
            }, scheduleDelaySecs, TimeUnit.SECONDS);
        });
    }

    private void doRetryCache() {
        try {
            for (Map.Entry<String, RetryMessage> entry : retryCache.asMap().entrySet()) {
                try {
                    RetryMessage retryMessage = entry.getValue();
                    Message message = retryMessage.message;
                    Session session = retryMessage.session;
                    int mqttMsgId = retryMessage.mqttMsgId;

                    if (System.currentTimeMillis() - retryMessage.timestamp < messageRetryInterval) {
                        continue;
                    }

                    if (MqttMessageType.PUBLISH.equals(retryMessage.mqttMessageType)) {
                        if (session == null || session.isDestroyed()) {
                            cleanRetryMessage(mqttMsgId, session.getChannelId());
                            continue;
                        }
                        if (retryMessage.mountTimeout()) {
                            saveRetryQueue(entry.getKey(), retryMessage);
                            cleanRetryMessage(mqttMsgId, session.getChannelId());
                            continue;
                        }
                        pushAction.write(session, message, mqttMsgId, retryMessage.qos, retryMessage.subscription);
                        retryMessage.timestamp = System.currentTimeMillis();
                        retryMessage.localRetryTime++;
                    } else if (MqttMessageType.PUBREL.equals(retryMessage.mqttMessageType)) {
                        if (session == null || session.isDestroyed() || retryMessage.mountRelTimeout()) {
                            retryCache.invalidate(entry.getKey());
                            logger.error("failed to retry pub rel more times,{},{}", session.getClientId(), mqttMsgId);
                            pushAction.rollNextByAck(session, mqttMsgId);
                            continue;
                        }
                        MqttFixedHeader pubRelMqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false,
                                MqttQoS.valueOf(retryMessage.qos), false, 0);
                        MqttMessage pubRelMqttMessage = new MqttMessage(pubRelMqttFixedHeader,
                                MqttMessageIdVariableHeader.from(mqttMsgId));
                        session.getChannel().writeAndFlush(pubRelMqttMessage);
                        retryMessage.localRetryTime++;
                        retryMessage.timestamp = System.currentTimeMillis();
                        logger.warn("retryPubRel:{},{}", session.getClientId(), mqttMsgId);
                    } else {
                        logger.error("error retry message type:{}", retryMessage.mqttMessageType);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void mountPublish(int mqttMsgId, Message message, int qos, String channelId, Subscription subscription) {
        Session session = sessionLoop.getSession(channelId);
        if (session == null) {
            return;
        }

        RetryMessage retryMessage = new RetryMessage(session, message, qos, mqttMsgId, MqttMessageType.PUBLISH, subscription);
        retryCache.put(buildKey(mqttMsgId, channelId), retryMessage);
        Map<Integer, RetryMessage> noWaitRetryMsgMap = sessionNoWaitRetryMsgMap.get(channelId);
        if (noWaitRetryMsgMap == null) {
            noWaitRetryMsgMap = new ConcurrentHashMap<>(2);
            Map<Integer, RetryMessage> old = sessionNoWaitRetryMsgMap.putIfAbsent(channelId, noWaitRetryMsgMap);
            if (old != null) {
                noWaitRetryMsgMap = old;
            }
        }

        if (subscription != null && !subscription.isRetry() &&
                noWaitRetryMsgMap.size() < connectConf.getSizeOfNotRollWhenAckSlow()) {
            noWaitRetryMsgMap.put(mqttMsgId, retryMessage);
            pushAction.rollNextNoWaitRetry(session, mqttMsgId);
        }
    }

    private RetryMessage cleanRetryMessage(int mqttMsgId, String channelId) {
        Map<Integer, RetryMessage> retryMsgMap = sessionNoWaitRetryMsgMap.get(channelId);
        if (retryMsgMap != null) {
            retryMsgMap.remove(mqttMsgId);
        }
        String key = buildKey(mqttMsgId, channelId);
        return unMount(key);
    }

    public void mountPubRel(int mqttMsgId, String channelId) {
        Session session = sessionLoop.getSession(channelId);
        if (session == null) {
            return;
        }
        RetryMessage retryMessage = new RetryMessage(session, null, MqttQoS.AT_LEAST_ONCE.value(), mqttMsgId,
                MqttMessageType.PUBREL, null);
        retryCache.put(buildKey(mqttMsgId, channelId), retryMessage);
    }

    public RetryMessage unMountPublish(int mqttMsgId, String channelId) {
        RetryMessage retryMessage = cleanRetryMessage(mqttMsgId, channelId);
        return retryMessage;
    }

    public RetryMessage unMountPubRel(int mqttMsgId, String channelId) {
        String key = buildKey(mqttMsgId, channelId);
        return unMount(key);
    }

    private RetryMessage unMount(String key) {
        RetryMessage message = retryCache.getIfPresent(key);
        if (message != null) {
            retryCache.invalidate(key);
        }
        return message;
    }

    public boolean needRetryBefore(Subscription subscription, Queue queue, Session session) {
        Map<Integer, InFlyCache.PendingDown> pendingDowns = inFlyCache.getPendingDownCache().all(
                session.getChannelId());
        if (pendingDowns == null || pendingDowns.isEmpty()) {
            return false;
        }
        InFlyCache.PendingDown pendingDown = null;
        for (Map.Entry<Integer, InFlyCache.PendingDown> entry : pendingDowns.entrySet()) {
            InFlyCache.PendingDown each = entry.getValue();
            if (each.getSubscription().equals(subscription) && each.getQueue().equals(queue)) {
                pendingDown = each;
                break;
            }
        }
        return pendingDown != null;
    }

    private String buildKey(int mqttMsgId, String channelId) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.valueOf(mqttMsgId));
        sb.append("_");
        sb.append(channelId);
        return sb.toString();
    }

    public class RetryMessage {
        private Session session;
        private Message message;
        private Subscription subscription;
        private int qos;
        private int mqttMsgId;
        private MqttMessageType mqttMessageType;
        private int localRetryTime = 0;
        private static final int MAX_LOCAL_RETRY = 1;
        private long timestamp = System.currentTimeMillis();

        public RetryMessage(Session session, Message message, int qos, int mqttMsgId, MqttMessageType mqttMessageType, Subscription subscription) {
            this.session = session;
            this.message = message;
            this.qos = qos;
            this.mqttMsgId = mqttMsgId;
            this.mqttMessageType = mqttMessageType;
            this.subscription = subscription;
        }

        private boolean mountTimeout() {
            return localRetryTime >= MAX_LOCAL_RETRY;
        }

        private boolean mountRelTimeout() {
            return localRetryTime >= 3;
        }
    }

}
