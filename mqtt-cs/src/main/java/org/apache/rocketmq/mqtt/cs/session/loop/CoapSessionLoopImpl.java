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

import com.alibaba.fastjson.JSONObject;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class CoapSessionLoopImpl implements CoapSessionLoop{
    private static Logger logger = LoggerFactory.getLogger(CoapSessionLoopImpl.class);

    @Resource
    private PushAction pushAction;

    @Resource
    private MatchAction matchAction;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private QueueFresh queueFresh;


    private ScheduledThreadPoolExecutor pullService;
    private ScheduledThreadPoolExecutor scheduler;
    private HashedWheelTimer hashedWheelTimer;

    private Map<InetSocketAddress, CoapSession> sessionMap = new ConcurrentHashMap<>(1024);
    private Map<String, PullEvent> pullEventMap = new ConcurrentHashMap<>(1024);
    private Map<String, Boolean> pullStatus = new ConcurrentHashMap<>(1024);

    private AtomicLong rid = new AtomicLong();
    private long pullIntervalMillis = 10;

    @PostConstruct
    public void init() {
        pullService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("coap_pull_message_thread_"));
        scheduler = new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("coap_loop_scheduler_"));
        pullService.scheduleWithFixedDelay(() -> pullLoop(), pullIntervalMillis, pullIntervalMillis, TimeUnit.MILLISECONDS);
        hashedWheelTimer = new HashedWheelTimer(1, TimeUnit.SECONDS);
        hashedWheelTimer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (InetSocketAddress address : sessionMap.keySet()) {
                removeSession(address);
            }
        }));
    }

    private  void pullLoop() {
        try {
            for (Map.Entry<String, PullEvent> entry : pullEventMap.entrySet()) {
                PullEvent pullEvent = entry.getValue();
                CoapSession session = pullEvent.session;
                if (Boolean.TRUE.equals(pullStatus.get(eventQueueKey(session, pullEvent.queue)))) {
                    continue;
                }
                doPull(pullEvent);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }


    @Override
    public void addSession(CoapSession session, CompletableFuture<Void> future) {
        // todo: addSubscriptionAndInit
        InetSocketAddress address = session.getAddress();
        synchronized (this) {
            // if this session is already exist, refresh the subscription time and do nothing
            if (sessionMap.containsKey(address)) {
                sessionMap.get(address).refreshSubscribeTime();
                return;
            }
            sessionMap.put(address, session);
        }
        // Init
        AtomicInteger result = new AtomicInteger(0);
        queueFresh.freshQueue(session);
        Map<Queue, QueueOffset> offsetMap = session.getOffsetMap();
        result.addAndGet(offsetMap.size());
        for (Map.Entry<Queue, QueueOffset> entry : offsetMap.entrySet()) {
            initOffset(session, entry.getKey(), entry.getValue(), future, result);
        }
        matchAction.addSubscription(session);
        hashedWheelTimer.newTimeout(timeout -> checkSessionAlive(timeout, address), connectConf.getCoapSessionTimeout(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CoapSession getSession(InetSocketAddress address) {
        return sessionMap.get(address);
    }

    @Override
    public CoapSession removeSession(InetSocketAddress address) {
        CoapSession session = null;
        try {
            synchronized (this) {
                session = sessionMap.remove(address);
            }
            // todo: inFlyCache.cleanResource()
            if (session != null) {
                matchAction.removeSubscription(session);
                // todo: persistOffset(session)
            }
        } catch (Exception e) {
            logger.error("unloadSession fail:{}", address, e);
        }
        return session;
    }

    @Override
    public void notifyPullMessage(CoapSession session, Queue queue) {
        if (session == null || queue == null) {
            return;
        }
        logger.info("session loop impl doing notifyPullMessage queueFresh.freshQueue({}, {}})", session, session.getSubscription());
        queueFresh.freshQueue(session);
        pullMessage(session, queue);
    }

    private void pullMessage(CoapSession session, Queue queue) {
        if (session == null || queue == null) {
            return;
        }
//        if (needLoadPersistedOffset(session, queue)) {
//            return;
//        }
        if (!session.sendingMessageIsEmpty(queue)) {
            scheduler.schedule(() -> pullMessage(session, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            PullEvent pullEvent = new PullEvent(session, queue);
            pullEventMap.put(eventQueueKey(session, queue), pullEvent);
        }
    }

    private void checkSessionAlive(Timeout timeout, InetSocketAddress address) {
        CoapSession session = sessionMap.get(address);
        if (session == null) {
            return;
        }
        if (System.currentTimeMillis() - session.getSubscribeTime() > connectConf.getCoapSessionTimeout()) {
            removeSession(address);
        } else {
            long delay = connectConf.getCoapSessionTimeout() - (System.currentTimeMillis() - session.getSubscribeTime());
            hashedWheelTimer.newTimeout(timeout.task(), delay, TimeUnit.MILLISECONDS);
        }
    }

    private void doPull(PullEvent pullEvent) {
        CoapSession session = pullEvent.session;
        Subscription subscription = session.getSubscription();
        Queue queue = pullEvent.queue;
        QueueOffset queueOffset = session.getQueueOffset(queue);
        if (session.getSubscription() == null || queueOffset == null) {
            clearPullStatus(session, queue, pullEvent);
            return;
        }

        if (!queueOffset.isInitialized()) {
            initOffset(session, queue, queueOffset, null, null);
            scheduler.schedule(() -> pullMessage(session, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
            return;
        }

        pullStatus.put(eventQueueKey(session, queue), true);
        int count = session.getPullSize() > 0 ? session.getPullSize() : connectConf.getPullBatchSize();
        CompletableFuture<PullResult> result = new CompletableFuture<>();
        result.whenComplete((pullResult, throwable) -> {
            if (throwable != null) {
                clearPullStatus(session, queue, pullEvent);
                logger.error("{}", session.getAddress(), throwable);
                scheduler.schedule(() -> pullMessage(session, queue), 1, TimeUnit.SECONDS);
                return;
            }
            try {
                if (PullResult.PULL_SUCCESS == pullResult.getCode()) {
                    if (pullResult.getMessageList() != null &&
                            pullResult.getMessageList().size() >= Math.min(count, connectConf.getMaxTransferCountOnMessageInDisk())) {
                        scheduler.schedule(() -> pullMessage(session, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
                    }
                    boolean add = session.addSendingMessages(queue, pullResult.getMessageList());
                    if (add) {
                        pushAction.coapMessageArrive(session, queue);
                    }
                } else if (PullResult.PULL_OFFSET_MOVED == pullResult.getCode()) {
                    queueOffset.setOffset(pullResult.getNextQueueOffset().getOffset());
//                    session.markPersistOffsetFlag(true);
                    pullMessage(session, queue);
                } else {
                    logger.error("response:{},{}", session.getAddress(), JSONObject.toJSONString(pullResult));
                }
            } finally {
                clearPullStatus(session, queue, pullEvent);
            }
        });

        CompletableFuture<PullResult> pullResult = lmqQueueStore.pullMessage(subscription.toFirstTopic(), queue, queueOffset, count);
        pullResult.whenComplete((pullResult1, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                result.complete(pullResult1);
            }
        });
    }

    private void clearPullStatus(CoapSession session, Queue queue, PullEvent pullEvent) {
        pullEventMap.remove(eventQueueKey(session, queue), pullEvent);
        pullStatus.remove(eventQueueKey(session, queue));
    }

    private void futureDone(CompletableFuture<Void> future, AtomicInteger result) {
        if (future == null) {
            return;
        }
        if (result == null) {
            return;
        }
        if (result.decrementAndGet() <= 0) {
            future.complete(null);
        }
    }

    private void initOffset(CoapSession session, Queue queue, QueueOffset queueOffset, CompletableFuture<Void> future, AtomicInteger result) {
        if (queueOffset.isInitialized()) {
            futureDone(future, result);
            return;
        }
        if (queueOffset.isInitializing()) {
            return;
        }
        queueOffset.setInitializing();
        CompletableFuture<Long> queryResult = lmqQueueStore.queryQueueMaxOffset(queue);
        queryResult.whenComplete((maxOffset, throwable) -> {
            if (throwable != null) {
                logger.error("queryQueueMaxId onException {}", queue.getQueueName(), throwable);
                QueueOffset thisQueueOffset = session.getQueueOffset(queue);
                if (thisQueueOffset != null) {
                    if (!thisQueueOffset.isInitialized()) {
                        thisQueueOffset.setOffset(Long.MAX_VALUE);
                    }
                    thisQueueOffset.setInitialized();
                }
                futureDone(future, result);
                return;
            }
            QueueOffset thisQueueOffset = session.getQueueOffset(queue);
            if (thisQueueOffset != null) {
                if (!thisQueueOffset.isInitialized()) {
                    thisQueueOffset.setOffset(maxOffset);
                }
                thisQueueOffset.setInitialized();
            }
            futureDone(future, result);
        });
    }

    private String eventQueueKey(CoapSession session, Queue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(session.getAddress());
        sb.append("-");
        sb.append(queue.getQueueId());
        sb.append("-");
        sb.append(queue.getQueueName());
        sb.append("-");
        sb.append(queue.getBrokerName());
        return sb.toString();
    }

    class PullEvent {
        private CoapSession session;
        private Queue queue;
        private long id = rid.getAndIncrement();

        public PullEvent(CoapSession session, Queue queue) {
            this.session = session;
            this.queue = queue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PullEvent pullEvent = (PullEvent) o;

            return id == pullEvent.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
