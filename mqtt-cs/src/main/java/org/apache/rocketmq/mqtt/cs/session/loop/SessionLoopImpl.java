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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqOffsetStore;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.PullResult;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.SpringUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@Component
public class SessionLoopImpl implements SessionLoop {
    private static Logger logger = LoggerFactory.getLogger(SessionLoopImpl.class);

    @Resource
    private PushAction pushAction;

    @Resource
    private MatchAction matchAction;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private QueueCache queueCache;

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private LmqOffsetStore lmqOffsetStore;

    @Resource
    private QueueFresh queueFresh;

    @Resource
    private MetaClient metaClient;

    @Resource
    private MqttMsgId mqttMsgId;

    private ChannelManager channelManager;
    private ScheduledThreadPoolExecutor pullService;
    private ScheduledThreadPoolExecutor scheduler;
    private ScheduledThreadPoolExecutor persistOffsetScheduler;
    private SubscriptionPersistManager subscriptionPersistManager;

    private ScheduledThreadPoolExecutor aliveService;

    /**
     * channelId->session
     */
    private Map<String, Session> sessionMap = new ConcurrentHashMap<>(1024);
    private Map<String, Map<String, Session>> clientMap = new ConcurrentHashMap<>(1024);
    private Map<String, PullEvent> pullEventMap = new ConcurrentHashMap<>(1024);
    private Map<String, Boolean> pullStatus = new ConcurrentHashMap<>(1024);

    private AtomicLong rid = new AtomicLong();
    private long pullIntervalMillis = 10;

    private long checkAliveIntervalMillis = 5 * 1000;

    @PostConstruct
    public void init() {
        pullService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("pull_message_thread_"));
        aliveService = new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("check_alive_thread_"));
        scheduler = new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("loop_scheduler_"));
        persistOffsetScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("persistOffset_scheduler_"));
        persistOffsetScheduler.scheduleWithFixedDelay(() -> persistAllOffset(true), 5000, 5000, TimeUnit.MILLISECONDS);
        pullService.scheduleWithFixedDelay(() -> pullLoop(), pullIntervalMillis, pullIntervalMillis, TimeUnit.MILLISECONDS);
        aliveService.scheduleWithFixedDelay(() -> csLoop(), checkAliveIntervalMillis, checkAliveIntervalMillis, TimeUnit.MILLISECONDS);
        aliveService.scheduleWithFixedDelay(() -> masterLoop(), checkAliveIntervalMillis, checkAliveIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void pullLoop() {
        try {
            for (Map.Entry<String, PullEvent> entry : pullEventMap.entrySet()) {
                PullEvent pullEvent = entry.getValue();
                Session session = pullEvent.session;
                if (!session.getChannel().isActive()) {
                    pullStatus.remove(eventQueueKey(session, pullEvent.queue));
                    pullEventMap.remove(entry.getKey());
                    continue;
                }
                if (Boolean.TRUE.equals(pullStatus.get(eventQueueKey(session, pullEvent.queue)))) {
                    continue;
                }
                if (!session.getChannel().isWritable()) {
                    continue;
                }
                doPull(pullEvent);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void csLoop() {
        String ip = IpUtil.getLocalAddressCompatible();
        String csKey = Constants.CS_ALIVE + Constants.CTRL_1 + ip;
        String masterKey = Constants.CS_MASTER;
        long currentTime = System.currentTimeMillis();

        metaClient.put(csKey, String.valueOf(currentTime).getBytes()).whenComplete((result, throwable) -> {
            if (result == null || throwable != null) {
                logger.error("{} fail to put csKey", csKey);
                return;
            }
            logger.info("put csKey {} {}", csKey, JSON.parseObject(new String(metaClient.bGet(csKey)), new TypeReference<String>() {
            }));
        });

        if (!metaClient.bContainsKey(masterKey)) {
            logger.info("metaClient does not have master");
            return;
        }
        byte[] masterValueBytes = metaClient.bGet(masterKey);
        if (masterValueBytes == null || masterValueBytes.length == 0) {
            return;
        }
        String masterValue = new String(masterValueBytes);
        String masterUpdateTime = masterValue.substring((ip + Constants.PLUS_SIGN).length());
        logger.info("masterUpdateTime is {}", masterUpdateTime);

        if (currentTime - Long.parseLong(masterUpdateTime) > 20 * checkAliveIntervalMillis) {
            metaClient.compareAndPut(masterKey, masterValueBytes, (ip + Constants.PLUS_SIGN + currentTime).getBytes()).whenComplete((result, throwable) -> {
                if (!result || throwable != null) {
                    logger.error("{} fail to update master", ip);
                    return;
                }
                logger.info("{} update master successfully", ip);
            });
        }

    }

    private void masterLoop() {
        String masterKey = Constants.CS_MASTER;
        String ip = IpUtil.getLocalAddressCompatible();
        long currentTime = System.currentTimeMillis();

        if (!metaClient.bContainsKey(masterKey)) {
//            metaClient.compareAndPut(masterKey, null, (ip+Constants.PLUS_SIGN+currentTime).getBytes()).whenComplete((result, throwable) -> {
            metaClient.put(masterKey, (ip + Constants.PLUS_SIGN + currentTime).getBytes()).whenComplete((result, throwable) -> {
                if (!result || throwable != null) {
                    logger.error("{} fail to update master", ip);
                    return;
                }
                logger.info("put master {}", JSON.parseObject(new String(metaClient.bGet(masterKey)), new TypeReference<String>() {
                }));
            });
        }
        logger.info("master is {}", new String(metaClient.bGet(Constants.CS_MASTER)));
        if (metaClient.bContainsKey(Constants.CS_MASTER) && !new String(metaClient.bGet(Constants.CS_MASTER)).startsWith(IpUtil.getLocalAddressCompatible())) {
            logger.info("master is not {}", IpUtil.getLocalAddressCompatible());
            return;
        }

        metaClient.put(masterKey, (ip + Constants.PLUS_SIGN + currentTime).getBytes()).whenComplete((result, throwable) -> {
            if (result == null || throwable != null) {
                logger.error("{} fail to put master", ip);
                return;
            }
            logger.info("put master  {} {}", ip, JSON.parseObject(new String(metaClient.bGet(masterKey)), new TypeReference<String>() {
            }));
        });

        String startCSKey = Constants.CS_ALIVE + Constants.CTRL_0;
        String endCSKey = Constants.CS_ALIVE + Constants.CTRL_2;
        metaClient.scan(startCSKey, endCSKey).whenComplete((result, throwable) -> {
            if (throwable != null) {
                logger.error("{} master fail to scan cs", ip);
                return;
            }

            if (result.size() == 0) {
                logger.info("master scanned 0 cs");
                return;
            }
            for (KVEntry kvEntry : result) {
                logger.info("master {} scan cs {} {}", ip, new String(kvEntry.getKey()), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(new String(kvEntry.getValue()))));
                if (System.currentTimeMillis() - Long.parseLong(new String(kvEntry.getValue())) > 20 * checkAliveIntervalMillis) {
                    String csIp = new String(kvEntry.getKey()).substring((Constants.CS_ALIVE + Constants.CTRL_1).length());
                    handleShutDownCS(csIp);
                }
            }
        });
        logger.info("{} update master successfully", ip);
    }

    private CompletableFuture handleShutDownCS(String ip) {
        CompletableFuture future = new CompletableFuture();

        String startClientKey = ip + Constants.CTRL_0;
        String endClientKey = ip + Constants.CTRL_2;
        metaClient.scan(startClientKey, endClientKey).whenComplete((willList, throwable) -> {
            if (throwable != null) {
                logger.error("{} master fail to scan cs", ip);
                return;
            }

            if (willList.size() == 0) {
                logger.info("master handle 0 will");
                future.complete(null);
                return;
            }
            for (KVEntry kvEntry : willList) {
                logger.info("master handle will {} {}", kvEntry.getKey(), kvEntry.getValue());
                String willKey = new String(kvEntry.getValue());
                String clientId = new String(kvEntry.getKey()).substring((ip + Constants.CTRL_1).length());
                WillMessage willMessage = JSON.parseObject(willKey, new TypeReference<WillMessage>() {
                });
                MQHandleWillMessage(willMessage, clientId);

                metaClient.delete(new String(kvEntry.getKey())).whenComplete((result, e) -> {
                    if (!result || e != null) {
                        logger.error("fail to delete will message key", willKey);
                        return;
                    }
                    logger.info("delete will message key {} successfully", willKey);
                });
            }
            future.complete(null);
        });
        return future;
    }

    /**
     * distribute will message through MQ
     *
     * @param willMessage
     * @param clientId
     * @return
     */
    private CompletableFuture<Boolean> MQHandleWillMessage(WillMessage willMessage, String clientId) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        int mqttId = mqttMsgId.nextId(clientId);
        MqttMessage mqttMessage = MessageUtil.toMqttMessage(willMessage.getWillTopic(), willMessage.getBody(), willMessage.getQos(), mqttId);
        Message message = MessageUtil.toMessage((MqttPublishMessage) mqttMessage);
        String willTopic = willMessage.getWillTopic();
        String msgId = MessageClientIDSetter.createUniqID();
        message.setMsgId(msgId);
        message.setBornTimestamp(System.currentTimeMillis());
        Set<String> queueNames = new HashSet<>();
        queueNames.add(willTopic);

        CompletableFuture<StoreResult> storeResultFuture = lmqQueueStore.putMessage(queueNames, message);
        storeResultFuture.whenComplete((storeResult, throwable) -> {
            logger.info("will message : {}, {}, {}", storeResult.getQueue().getBrokerName(), storeResult.getQueue().getQueueName(), storeResult.getMsgId());
            if (throwable != null) {
                logger.error("fail to send will message : {}, {}, {}", storeResult.getQueue().getBrokerName(), storeResult.getQueue().getQueueName(), storeResult.getMsgId());
            }
            result.complete(throwable == null);
        });

        return result;
    }

    @Override
    public void setChannelManager(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    public void loadSession(String clientId, Channel channel) {
        if (StringUtils.isBlank(clientId)) {
            return;
        }
        if (!channel.isActive()) {
            return;
        }
        String channelId = ChannelInfo.getId(channel);
        if (sessionMap.containsKey(channelId)) {
            return;
        }
        Session session = new Session();
        session.setClientId(clientId);
        session.setChannelId(channelId);
        session.setChannel(channel);
        addSubscriptionAndInit(session,
                new HashSet<>(
                        Arrays.asList(Subscription.newP2pSubscription(clientId), Subscription.newRetrySubscription(clientId))),
                ChannelInfo.getFuture(channel, ChannelInfo.FUTURE_CONNECT));
        synchronized (this) {
            sessionMap.put(channelId, session);
            if (!clientMap.containsKey(clientId)) {
                clientMap.putIfAbsent(clientId, new ConcurrentHashMap<>(2));
            }
            clientMap.get(clientId).put(channelId, session);
        }
        if (!channel.isActive()) {
            unloadSession(clientId, channelId);
            return;
        }
        if (!session.isClean()) {
            notifyPullMessage(session, null, null);
        }
    }

    @Override
    public Session unloadSession(String clientId, String channelId) {
        Session session = null;
        try {
            synchronized (this) {
                session = sessionMap.remove(channelId);
                if (clientId == null) {
                    if (session != null) {
                        clientId = session.getClientId();
                    }
                }
                if (clientId != null && clientMap.containsKey(clientId)) {
                    clientMap.get(clientId).remove(channelId);
                    if (clientMap.get(clientId).isEmpty()) {
                        clientMap.remove(clientId);
                    }
                }
            }
            inFlyCache.cleanResource(clientId, channelId);
            if (session != null) {
                matchAction.removeSubscription(session, session.subscriptionSnapshot());
                persistOffset(session);
            }
        } catch (Exception e) {
            logger.error("unloadSession fail:{},{}", clientId, channelId, e);
        } finally {
            if (session != null) {
                session.destroy();
            }
        }
        return session;
    }

    @Override
    public Session getSession(String channelId) {
        return sessionMap.get(channelId);
    }

    @Override
    public List<Session> getSessionList(String clientId) {
        List<Session> list = new ArrayList<>();
        Map<String, Session> sessions = clientMap.get(clientId);
        if (sessions != null && !sessions.isEmpty()) {
            for (Session session : sessions.values()) {
                if (!session.isDestroyed()) {
                    list.add(session);
                } else {
                    logger.error("the session was destroyed,{}", clientId);
                    sessions.remove(session.getChannelId());
                }
            }
        }
        return list;
    }

    @Override
    public void addSubscription(String channelId, Set<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        Session session = getSession(channelId);
        if (session == null) {
            return;
        }
        addSubscriptionAndInit(session, subscriptions,
                ChannelInfo.getFuture(session.getChannel(), ChannelInfo.FUTURE_SUBSCRIBE));
        matchAction.addSubscription(session, subscriptions);

        if (!session.isClean()) {
            for (Subscription subscription : subscriptions) {
                notifyPullMessage(session, subscription, null);
            }
        }
    }

    @Override
    public void removeSubscription(String channelId, Set<Subscription> subscriptions) {
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        Session session = getSession(channelId);
        if (session == null) {
            return;
        }
        for (Subscription subscription : subscriptions) {
            session.removeSubscription(subscription);
        }
        matchAction.removeSubscription(session, subscriptions);
    }

    private void addSubscriptionAndInit(Session session, Set<Subscription> subscriptions,
                                        CompletableFuture<Void> future) {
        if (session == null) {
            return;
        }
        if (subscriptions == null) {
            return;
        }
        session.addSubscription(subscriptions);
        AtomicInteger result = new AtomicInteger(subscriptions.size());
        for (Subscription subscription : subscriptions) {
            queueFresh.freshQueue(session, subscription);
            Map<Queue, QueueOffset> queueOffsets = session.getQueueOffset(subscription);
            if (queueOffsets != null) {
                for (Map.Entry<Queue, QueueOffset> entry : queueOffsets.entrySet()) {
                    initOffset(session, subscription, entry.getKey(), entry.getValue(), future, result);
                }
            }
        }
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

    private void initOffset(Session session, Subscription subscription, Queue queue, QueueOffset queueOffset,
                            CompletableFuture<Void> future, AtomicInteger result) {
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
                QueueOffset thisQueueOffset = session.getQueueOffset(subscription, queue);
                if (thisQueueOffset != null) {
                    if (!thisQueueOffset.isInitialized()) {
                        thisQueueOffset.setOffset(Long.MAX_VALUE);
                    }
                    thisQueueOffset.setInitialized();
                }
                futureDone(future, result);
                return;
            }
            QueueOffset thisQueueOffset = session.getQueueOffset(subscription, queue);
            if (thisQueueOffset != null) {
                if (!thisQueueOffset.isInitialized()) {
                    thisQueueOffset.setOffset(maxOffset);
                }
                thisQueueOffset.setInitialized();
            }
            futureDone(future, result);
        });
    }

    @Override
    public void notifyPullMessage(Session session, Subscription subscription, Queue queue) {
        if (session == null || session.isDestroyed()) {
            return;
        }
        if (subscriptionPersistManager == null) {
            subscriptionPersistManager = SpringUtils.getBean(SubscriptionPersistManager.class);
        }
        if (subscriptionPersistManager != null &&
                !session.isClean() && !session.isLoaded()) {
            if (session.isLoading()) {
                return;
            }
            session.setLoading();
            CompletableFuture<Set<Subscription>> future = subscriptionPersistManager.loadSubscriptions(session.getClientId());
            future.whenComplete((subscriptions, throwable) -> {
                if (throwable != null) {
                    logger.error("", throwable);
                    scheduler.schedule(() -> {
                        session.resetLoad();
                        notifyPullMessage(session, subscription, queue);
                    }, 3, TimeUnit.SECONDS);
                    return;
                }
                session.addSubscription(subscriptions);
                matchAction.addSubscription(session, subscriptions);
                session.setLoaded();
                notifyPullMessage(session, subscription, queue);
            });
            return;
        }
        if (queue != null) {
            if (subscription == null) {
                throw new RuntimeException(
                        "invalid notifyPullMessage, subscription is null, but queue is not null," + session.getClientId());
            }
            queueFresh.freshQueue(session, subscription);
            pullMessage(session, subscription, queue);
            return;
        }
        for (Subscription each : session.subscriptionSnapshot()) {
            if (subscription != null && !each.equals(subscription)) {
                continue;
            }
            queueFresh.freshQueue(session, each);
            Map<Queue, QueueOffset> queueOffsets = session.getQueueOffset(each);
            if (queueOffsets != null) {
                for (Map.Entry<Queue, QueueOffset> entry : queueOffsets.entrySet()) {
                    pullMessage(session, each, entry.getKey());
                }
            }
        }
    }

    @Override
    public void addWillMessage(Channel channel, WillMessage willMessage) {
        Session session = getSession(ChannelInfo.getId(channel));
        String clientId = ChannelInfo.getClientId(channel);
        String ip = IpUtil.getLocalAddressCompatible();

        if (session == null) {
            return;
        }
        if (willMessage == null) {
            return;
        }

        String topic = willMessage.getWillTopic();
        String message = JSON.toJSONString(willMessage);
        String willKey = ip + Constants.CTRL_1 + clientId;

        if (metaClient.bContainsKey(willKey)) {
            logger.error("client {} already have will topic {} will message {}", clientId, topic, willMessage);
            return;
        }

        // key: ip + clientId; value: WillMessage
        metaClient.put(willKey, message.getBytes()).whenComplete((result, throwable) -> {
            if (!result || throwable != null) {
                logger.error("fail to put will message key {} value {}", willKey, willMessage);
                return;
            }
            logger.info("put will message key {} value {} successfully", willKey, JSON.parseObject(new String(metaClient.bGet(willKey)), new TypeReference<WillMessage>() {
            }));
        });
    }

    private String eventQueueKey(Session session, Queue queue) {
        StringBuilder sb = new StringBuilder();
        sb.append(ChannelInfo.getId(session.getChannel()));
        sb.append("-");
        sb.append(queue.getQueueId());
        sb.append("-");
        sb.append(queue.getQueueName());
        sb.append("-");
        sb.append(queue.getBrokerName());
        return sb.toString();
    }

    private boolean needLoadPersistedOffset(Session session, Subscription subscription, Queue queue) {
        if (session.isClean()) {
            return false;
        }
        Integer status = session.getLoadStatusMap().get(subscription);
        if (status != null && status == 1) {
            return false;
        }
        if (status != null && status == 0) {
            return true;
        }
        session.getLoadStatusMap().put(subscription, 0);
        CompletableFuture<Map<Queue, QueueOffset>> result = lmqOffsetStore.getOffset(session.getClientId(), subscription);
        result.whenComplete((offsetMap, throwable) -> {
            if (throwable != null) {
                // retry
                scheduler.schedule(() -> {
                    session.getLoadStatusMap().put(subscription, -1);
                    pullMessage(session, subscription, queue);
                }, 3, TimeUnit.SECONDS);
                return;
            }
            session.addOffset(subscription.toQueueName(), offsetMap);
            session.getLoadStatusMap().put(subscription, 1);
            pullMessage(session, subscription, queue);
        });
        return true;
    }

    private void pullMessage(Session session, Subscription subscription, Queue queue) {
        if (queue == null) {
            return;
        }
        if (session == null || session.isDestroyed()) {
            return;
        }
        if (needLoadPersistedOffset(session, subscription, queue)) {
            return;
        }
        if (!session.sendingMessageIsEmpty(subscription, queue)) {
            scheduler.schedule(() -> pullMessage(session, subscription, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
        } else {
            PullEvent pullEvent = new PullEvent(session, subscription, queue);
            pullEventMap.put(eventQueueKey(session, queue), pullEvent);
        }
    }

    private void doPull(PullEvent pullEvent) {
        Session session = pullEvent.session;
        Subscription subscription = pullEvent.subscription;
        Queue queue = pullEvent.queue;
        QueueOffset queueOffset = session.getQueueOffset(subscription, queue);
        if (session.isDestroyed() || queueOffset == null) {
            clearPullStatus(session, queue, pullEvent);
            return;
        }

        if (!queueOffset.isInitialized()) {
            initOffset(session, subscription, queue, queueOffset, null, null);
            scheduler.schedule(() -> pullMessage(session, subscription, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
            return;
        }

        pullStatus.put(eventQueueKey(session, queue), true);
        int count = session.getPullSize() > 0 ? session.getPullSize() : connectConf.getPullBatchSize();
        CompletableFuture<PullResult> result = new CompletableFuture<>();
        result.whenComplete((pullResult, throwable) -> {
            if (throwable != null) {
                clearPullStatus(session, queue, pullEvent);
                logger.error("{}", session.getClientId(), throwable);
                if (session.isDestroyed()) {
                    return;
                }
                scheduler.schedule(() -> pullMessage(session, subscription, queue), 1, TimeUnit.SECONDS);
                return;
            }
            try {
                if (session.isDestroyed()) {
                    return;
                }
                if (PullResult.PULL_SUCCESS == pullResult.getCode()) {
                    if (pullResult.getMessageList() != null &&
                            pullResult.getMessageList().size() >= count) {
                        scheduler.schedule(() -> pullMessage(session, subscription, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
                    }
                    boolean add = session.addSendingMessages(subscription, queue, pullResult.getMessageList());
                    if (add) {
                        pushAction.messageArrive(session, subscription, queue);
                    }
                } else if (PullResult.PULL_OFFSET_MOVED == pullResult.getCode()) {
                    queueOffset.setOffset(pullResult.getNextQueueOffset().getOffset());
                    queueOffset.setOffset(pullResult.getNextQueueOffset().getOffset());
                    session.markPersistOffsetFlag(true);
                    pullMessage(session, subscription, queue);
                } else {
                    logger.error("response:{},{}", session.getClientId(), JSONObject.toJSONString(pullResult));
                }
            } finally {
                clearPullStatus(session, queue, pullEvent);
            }
        });

        PullResultStatus pullResultStatus = queueCache.pullMessage(session, subscription, queue, queueOffset, count, result);
        if (PullResultStatus.LATER.equals(pullResultStatus)) {
            clearPullStatus(session, queue, pullEvent);
            scheduler.schedule(() -> pullMessage(session, subscription, queue), pullIntervalMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void clearPullStatus(Session session, Queue queue, PullEvent pullEvent) {
        pullEventMap.remove(eventQueueKey(session, queue), pullEvent);
        pullStatus.remove(eventQueueKey(session, queue));
    }

    private void persistAllOffset(boolean needSleep) {
        try {
            for (Session session : sessionMap.values()) {
                if (persistOffset(session) && needSleep) {
                    Thread.sleep(5L);
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private boolean persistOffset(Session session) {
        try {
            if (!session.getPersistOffsetFlag()) {
                return false;
            }
            lmqOffsetStore.save(session.getClientId(), session.offsetMapSnapshot());
        } catch (Exception e) {
            logger.error("{}", session.getClientId(), e);
        }
        return true;
    }

    class PullEvent {
        private Session session;
        private Subscription subscription;
        private Queue queue;
        private long id = rid.getAndIncrement();

        public PullEvent(Session session, Subscription subscription, Queue queue) {
            this.session = session;
            this.subscription = subscription;
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
