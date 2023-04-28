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

package org.apache.rocketmq.mqtt.cs.session.loop;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.StatUtil;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.exporter.collector.MqttMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.mqtt.cs.session.loop.PullResultStatus.DONE;
import static org.apache.rocketmq.mqtt.cs.session.loop.PullResultStatus.LATER;


@Component
public class QueueCache {
    private static Logger logger = LoggerFactory.getLogger(QueueCache.class);

    @Resource
    private ConnectConf connectConf;

    @Resource
    private LmqQueueStore lmqQueueStore;

    private ScheduledThreadPoolExecutor loadCacheService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("loadCache_"));

    private AtomicLong rid = new AtomicLong();
    private Map<Queue, QueueEvent> loadEvent = new ConcurrentHashMap<>();
    private Map<Queue, Boolean> loadStatus = new ConcurrentHashMap<>();

    private Cache<Queue, CacheEntry> cache = Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .maximumSize(1_000)
            .build();


    @PostConstruct
    public void init() {
        loadCacheService.scheduleWithFixedDelay(() -> {
            for (Map.Entry<Queue, QueueEvent> entry : loadEvent.entrySet()) {
                Queue queue = entry.getKey();
                QueueEvent event = entry.getValue();
                if (Boolean.TRUE.equals(loadStatus.get(queue))) {
                    continue;
                }
                CacheEntry cacheEntry = cache.getIfPresent(queue);
                if (cacheEntry == null) {
                    cacheEntry = new CacheEntry();
                    cache.put(queue, cacheEntry);
                }
                if (CollectionUtils.isEmpty(cacheEntry.messageList)) {
                    loadCache(true, queue.toFirstTopic(), queue, null, 1, event);
                    continue;
                }
                QueueOffset queueOffset = new QueueOffset();
                Message lastMessage = cacheEntry.messageList.get(cacheEntry.messageList.size() - 1);
                queueOffset.setOffset(lastMessage.getOffset() + 1);
                loadCache(false, queue.toFirstTopic(), queue, queueOffset, connectConf.getPullBatchSize(), event);
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    public void refreshCache(Pair<Queue, Session> pair) {
        Queue queue = pair.getLeft();
        if (queue == null) {
            return;
        }
        if (queue.isP2p() || queue.isRetry() || queue.isShare()) {
            return;
        }
        addLoadEvent(queue, pair.getRight());
    }

    class QueueEvent {
        long id;
        Session session;

        public QueueEvent(long id, Session session) {
            this.id = id;
            this.session = session;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            QueueEvent that = (QueueEvent) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private void addLoadEvent(Queue queue, Session session) {
        loadEvent.put(queue, new QueueEvent(rid.incrementAndGet(), session));
        CacheEntry cacheEntry = cache.getIfPresent(queue);
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            cache.put(queue, cacheEntry);
        }
    }

    private void callbackResult(CompletableFuture<PullResult> pullResult, CompletableFuture<PullResult> callBackResult) {
        pullResult.whenComplete((pullResult1, throwable) -> {
            if (throwable != null) {
                callBackResult.completeExceptionally(throwable);
            } else {
                callBackResult.complete(pullResult1);
            }
        });
    }

    private String toFirstTopic(Subscription subscription) {
        String firstTopic = subscription.toFirstTopic();
        if (subscription.isRetry()) {
            firstTopic = lmqQueueStore.getClientRetryTopic();
        }
        if (subscription.isP2p()) {
            if (StringUtils.isNotBlank(lmqQueueStore.getClientP2pTopic())) {
                firstTopic = lmqQueueStore.getClientP2pTopic();
            } else {
                firstTopic = lmqQueueStore.getClientRetryTopic();
            }
        }
        return firstTopic;
    }

    public PullResultStatus pullMessage(Session session, Subscription subscription, Queue queue,
                                        QueueOffset queueOffset, int count,
                                        CompletableFuture<PullResult> callBackResult) {
        if (subscription.isP2p() || subscription.isRetry()) {
            StatUtil.addPv("NotPullCache", 1);
            collectorPullCacheStatus("NotPullCache");
            CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }

        if (subscription.isShare()) {
            CompletableFuture<PullResult> pullResult = lmqQueueStore.popMessage(subscription.getSharedName(), toFirstTopic(subscription), queue, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }

        CacheEntry cacheEntry = cache.getIfPresent(queue);
        if (cacheEntry == null) {
            StatUtil.addPv("NoPullCache", 1);
            collectorPullCacheStatus("NotPullCache");
            CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }
        if (cacheEntry.loading.get()) {
            if (System.currentTimeMillis() - cacheEntry.startLoadingT > 1000) {
                StatUtil.addPv("LoadPullCacheTimeout", 1);
                collectorPullCacheStatus("LoadPullCacheTimeout");
                CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
                callbackResult(pullResult, callBackResult);
                return DONE;
            }
            return LATER;
        }

        List<Message> cacheMsgList = cacheEntry.messageList;
        if (cacheMsgList.isEmpty()) {
            if (loadEvent.get(queue) != null) {
                collectorPullCacheStatus("EmptyPullCacheLATER");
                StatUtil.addPv("EmptyPullCacheLATER", 1);
                return LATER;
            }
            StatUtil.addPv("EmptyPullCache", 1);
            collectorPullCacheStatus("EmptyPullCache");
            CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }

        if (queueOffset.getOffset() < cacheMsgList.get(0).getOffset()) {
            StatUtil.addPv("OutPullCache", 1);
            collectorPullCacheStatus("OutPullCache");
            CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }
        List<Message> resultMsgs = new ArrayList<>();
        synchronized (cacheEntry) {
            for (Message message : cacheMsgList) {
                if (message.getOffset() >= queueOffset.getOffset()) {
                    resultMsgs.add(message);
                }
                if (resultMsgs.size() >= count) {
                    break;
                }
            }
        }
        if (resultMsgs.isEmpty()) {
            if (loadEvent.get(queue) != null) {
                StatUtil.addPv("PullCacheLATER", 1);
                collectorPullCacheStatus("PullCacheLATER");
                return LATER;
            }
            StatUtil.addPv("OutPullCache2", 1);
            collectorPullCacheStatus("OutPullCache2");
            CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(toFirstTopic(subscription), queue, queueOffset, count);
            callbackResult(pullResult, callBackResult);
            return DONE;
        }
        PullResult pullResult = new PullResult();
        pullResult.setMessageList(resultMsgs);
        callBackResult.complete(pullResult);
        StatUtil.addPv("PullFromCache", 1);
        collectorPullCacheStatus("PullFromCache");
        if (loadEvent.get(queue) != null) {
            return LATER;
        }
        return DONE;
    }

    private void collectorPullCacheStatus(String pullCacheStatus) {
        try {
            MqttMetricsCollector.collectPullCacheStatusTps(1, pullCacheStatus);
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    private void loadCache(boolean isFirst, String firstTopic, Queue queue, QueueOffset queueOffset, int count,
                           QueueEvent event) {
        loadStatus.put(queue, true);
        CacheEntry cacheEntry = cache.getIfPresent(queue);
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry();
            cache.put(queue, cacheEntry);
        }
        cacheEntry.startLoad();
        CacheEntry finalCacheEntry = cacheEntry;
        CompletableFuture<PullResult> result;
        if (isFirst) {
            result = lmqQueueStore.pullLastMessages(firstTopic, queue, count);
        } else {
            result = lmqQueueStore.pullMessage(firstTopic, queue, queueOffset, count);
        }
        result.whenComplete((pullResult, throwable) -> {
            if (throwable != null) {
                logger.error("", throwable);
                loadEvent.remove(queue, event);
                loadStatus.remove(queue);
                finalCacheEntry.endLoad();
                addLoadEvent(queue, event.session);
                return;
            }
            try {
                if (pullResult != null && !CollectionUtils.isEmpty(pullResult.getMessageList())) {
                    synchronized (finalCacheEntry) {
                        finalCacheEntry.messageList.addAll(pullResult.getMessageList());
                        if (isFirst) {
                            Collections.sort(finalCacheEntry.messageList, Comparator.comparingLong(Message::getOffset));
                        }
                        int overNum = finalCacheEntry.messageList.size() - connectConf.getQueueCacheSize();
                        for (int i = 0; i < overNum; i++) {
                            finalCacheEntry.messageList.remove(0);
                        }
                    }
                    if (pullResult.getMessageList().size() >= count && !isFirst) {
                        addLoadEvent(queue, event.session);
                        return;
                    }
                }
            } catch (Exception e) {
                logger.error("loadCache failed {}", queue.getQueueName(), e);
                addLoadEvent(queue, event.session);
            } finally {
                loadEvent.remove(queue, event);
                loadStatus.remove(queue);
                finalCacheEntry.endLoad();
            }
        });
    }

    class CacheEntry {
        private AtomicBoolean loading = new AtomicBoolean(false);
        private List<Message> messageList = new ArrayList<>();
        private volatile long startLoadingT = System.currentTimeMillis();

        private void startLoad() {
            if (loading.compareAndSet(false, true)) {
                startLoadingT = System.currentTimeMillis();
            }
        }

        private void endLoad() {
            loading.compareAndSet(true, false);
        }
    }

}
