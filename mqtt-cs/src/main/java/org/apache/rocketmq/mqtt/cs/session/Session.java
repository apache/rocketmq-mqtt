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

package org.apache.rocketmq.mqtt.cs.session;


import io.netty.channel.Channel;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Session {
    private static Logger logger = LoggerFactory.getLogger(Session.class);
    private final long startTime = System.currentTimeMillis();
    private Channel channel;
    private volatile boolean destroyed = false;
    private volatile int loadStatus = -1;
    private volatile int pullSize;
    private String clientId;
    private String channelId;
    private AtomicBoolean needPersistOffset = new AtomicBoolean(false);
    private ConcurrentMap<String, Map<Queue, QueueOffset>> offsetMap = new ConcurrentHashMap<>(16);
    private Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private ConcurrentMap<Subscription, Map<Queue, LinkedHashSet<Message>>> sendingMessages = new ConcurrentHashMap<>(16);
    private ConcurrentMap<Subscription, Integer> loadStatusMap = new ConcurrentHashMap<>();

    public Session() {
    }

    public ConcurrentMap<Subscription, Integer> getLoadStatusMap() {
        return loadStatusMap;
    }

    public long getStartTime() {
        return startTime;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public boolean isClean() {
        return Boolean.TRUE.equals(ChannelInfo.getCleanSessionFlag(channel));
    }

    public boolean isLoaded() {
        return this.loadStatus == 1;
    }

    public void setLoaded() {
        this.loadStatus = 1;
    }

    public void setLoading() {
        this.loadStatus = 0;
    }

    public boolean isLoading() {
        return loadStatus == 0;
    }

    public void resetLoad() {
        this.loadStatus = -1;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    public int getPullSize() {
        return pullSize;
    }

    public void setPullSize(int pullSize) {
        this.pullSize = pullSize;
    }

    public void destroy() {
        this.destroyed = true;
        this.offsetMap.clear();
        this.sendingMessages.clear();
        this.subscriptions.clear();
    }

    public Map<Subscription, Map<Queue, QueueOffset>> offsetMapSnapshot() {
        Map<Subscription, Map<Queue, QueueOffset>> tmp = new HashMap<>(8);
        for (String queueName : offsetMap.keySet()) {
            Subscription subscription = subscriptions.get(queueName);
            if (subscription == null) {
                continue;
            }
            Map<Queue, QueueOffset> queueMap = new HashMap<>(8);
            tmp.put(subscription, queueMap);
            for (Map.Entry<Queue, QueueOffset> entry : offsetMap.get(queueName).entrySet()) {
                queueMap.put(entry.getKey(), entry.getValue());
            }
        }
        return tmp;
    }

    public Set<Subscription> subscriptionSnapshot() {
        Set<Subscription> tmp = new HashSet<>();
        tmp.addAll(subscriptions.values());
        return tmp;
    }

    public void removeSubscription(Subscription subscription) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        offsetMap.remove(subscription.toQueueName());
        sendingMessages.remove(subscription);
        subscriptions.remove(subscription.getTopicFilter());
    }

    public void freshQueue(Subscription subscription, Set<Queue> queues) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queues == null) {
            logger.warn("queues is null when freshQueue,{},{}", getClientId(), subscription);
            return;
        }
        if (!subscriptions.containsKey(subscription.getTopicFilter())) {
            return;
        }

        String queueName = subscription.toQueueName();
        if (!offsetMap.containsKey(queueName)) {
            offsetMap.putIfAbsent(queueName, new ConcurrentHashMap<>(16));
        }
        for (Queue memQueue : offsetMap.get(queueName).keySet()) {
            if (!queues.contains(memQueue)) {
                offsetMap.get(queueName).remove(memQueue);
            }
        }
        // init queueOffset
        for (Queue nowQueue : queues) {
            if (!offsetMap.get(queueName).containsKey(nowQueue)) {
                QueueOffset queueOffset = new QueueOffset();
                //if no offset  use init offset
                offsetMap.get(queueName).put(nowQueue, queueOffset);
                this.markPersistOffsetFlag(true);
            }
        }

        if (!sendingMessages.containsKey(subscription)) {
            sendingMessages.putIfAbsent(subscription, new ConcurrentHashMap<>(16));
        }
        for (Queue memQueue : sendingMessages.get(subscription).keySet()) {
            if (!queues.contains(memQueue)) {
                sendingMessages.get(subscription).remove(memQueue);
            }
        }
        if (queues.isEmpty()) {
            logger.warn("queues is empty when freshQueue,{},{}", getClientId(), subscription);
        }
    }

    public void addOffset(String queueName, Map<Queue, QueueOffset> map) {
        if (queueName == null) {
            throw new RuntimeException("queueName is null");
        }

        if (!offsetMap.containsKey(queueName)) {
            offsetMap.putIfAbsent(queueName, new ConcurrentHashMap<>(16));
        }
        offsetMap.get(queueName).putAll(map);
    }

    public void addOffset(Map<String, Map<Queue, QueueOffset>> offsetMapParam) {
        if (offsetMapParam != null && !offsetMapParam.isEmpty()) {
            for (String queueName : offsetMapParam.keySet()) {
                if (!subscriptions.containsKey(queueName)) {
                    continue;
                }
                addOffset(queueName, offsetMapParam.get(queueName));
            }
        }
    }

    public void addSubscription(Set<Subscription> subscriptionsParam) {
        if (CollectionUtils.isEmpty(subscriptionsParam)) {
            return;
        }
        for (Subscription subscription : subscriptionsParam) {
            addSubscription(subscription);
        }
    }

    public void addSubscription(Subscription subscriptionParam) {
        if (subscriptionParam != null) {
            subscriptions.put(subscriptionParam.getTopicFilter(), subscriptionParam);
        }
    }

    public QueueOffset getQueueOffset(Subscription subscription, Queue queue) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        String queueName = subscription.toQueueName();
        Map<Queue, QueueOffset> queueQueueOffsetMap = offsetMap.get(queueName);
        if (queueQueueOffsetMap != null) {
            return queueQueueOffsetMap.get(queue);
        }
        return null;
    }

    public Map<Queue, QueueOffset> getQueueOffset(Subscription subscription) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        String queueName = subscription.toQueueName();
        return offsetMap.get(queueName);
    }

    public boolean addSendingMessages(Subscription subscription, Queue queue, List<Message> messages) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        if (messages == null || messages.isEmpty()) {
            return false;
        }
        if (!subscriptions.containsKey(subscription.getTopicFilter())) {
            return false;
        }
        if (!sendingMessages.containsKey(subscription)) {
            sendingMessages.putIfAbsent(subscription, new ConcurrentHashMap<>(16));
        }
        if (!sendingMessages.get(subscription).containsKey(queue)) {
            sendingMessages.get(subscription).putIfAbsent(queue, new LinkedHashSet<>(8));
        }
        String queueName = subscription.toQueueName();
        Map<Queue, QueueOffset> queueOffsetMap = offsetMap.get(queueName);
        if (queueOffsetMap == null || !queueOffsetMap.containsKey(queue)) {
            logger.warn("not found queueOffset,{},{},{}", getClientId(), subscription, queue);
            return false;
        }
        boolean add = false;
        QueueOffset queueOffset;
        queueOffset = queueOffsetMap.get(queue);
        Iterator<Message> iterator = messages.iterator();
        while (iterator.hasNext()) {
            Message message = iterator.next();
            if (message.getOffset() < queueOffset.getOffset() && queueOffset.getOffset() != Long.MAX_VALUE) {
                continue;
            }
            synchronized (this) {
                if (sendingMessages.get(subscription).get(queue).add(message.copy())) {
                    add = true;
                }
            }
        }
        return add;
    }

    public Message rollNext(Subscription subscription, Queue pendingQueue, long pendingDownSeqId) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (pendingQueue == null) {
            throw new RuntimeException("queue is null");
        }
        Map<Queue, LinkedHashSet<Message>> queueSendingMsgs = sendingMessages.get(subscription);
        if (queueSendingMsgs == null || queueSendingMsgs.isEmpty()) {
            return null;
        }
        LinkedHashSet<Message> messages = queueSendingMsgs.get(pendingQueue);
        if (messages == null) {
            return null;
        }
        Message message;
        Message nextMessage = null;
        synchronized (this) {
            if (messages.isEmpty()) {
                return null;
            }
            message = messages.iterator().next();
            if (message.getOffset() != pendingDownSeqId) {
                return null;
            }
            messages.remove(message);
            updateQueueOffset(subscription, pendingQueue, message);
            this.markPersistOffsetFlag(true);
            if (!messages.isEmpty()) {
                nextMessage = messages.iterator().next();
            }
        }
        return nextMessage;
    }

    public boolean sendingMessageIsEmpty(Subscription subscription, Queue queue) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        Map<Queue, LinkedHashSet<Message>> queueSendingMsgs = sendingMessages.get(subscription);
        if (queueSendingMsgs == null || queueSendingMsgs.isEmpty()) {
            return true;
        }
        LinkedHashSet<Message> messages = queueSendingMsgs.get(queue);
        if (messages == null) {
            return true;
        }
        synchronized (this) {
            return messages.size() <= 0;
        }
    }

    public Message nextSendMessageByOrder(Subscription subscription, Queue queue) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        Map<Queue, LinkedHashSet<Message>> tmp = sendingMessages.get(subscription);
        if (tmp != null && !tmp.isEmpty()) {
            LinkedHashSet<Message> messages = tmp.get(queue);
            if (messages == null) {
                return null;
            }
            synchronized (this) {
                return messages.isEmpty() ? null : messages.iterator().next();
            }
        }
        return null;
    }

    public List<Message> pendMessageList(Subscription subscription, Queue queue) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        List<Message> list = new ArrayList<>();
        Map<Queue, LinkedHashSet<Message>> tmp = sendingMessages.get(subscription);
        if (tmp != null && !tmp.isEmpty()) {
            LinkedHashSet<Message> messages = tmp.get(queue);
            if (messages == null) {
                return null;
            }
            synchronized (this) {
                if (!messages.isEmpty()) {
                    for (Message message : messages) {
                        if (message.getAck() == -1) {
                            list.add(message);
                        }
                    }
                }
            }
        }
        return list;
    }

    public void ack(Subscription subscription, Queue pendingQueue, long pendingDownSeqId) {
        if (subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (pendingQueue == null) {
            throw new RuntimeException("queue is null");
        }
        Map<Queue, LinkedHashSet<Message>> queueSendingMsgs = sendingMessages.get(subscription);
        if (queueSendingMsgs == null || queueSendingMsgs.isEmpty()) {
            return;
        }
        LinkedHashSet<Message> messages = queueSendingMsgs.get(pendingQueue);
        if (messages == null) {
            return;
        }
        synchronized (this) {
            if (messages.isEmpty()) {
                return;
            }
            boolean flag = true;
            Iterator<Message> iterator = messages.iterator();
            while (iterator.hasNext()) {
                Message message = iterator.next();
                if (message.getOffset() == pendingDownSeqId) {
                    message.setAck(1);
                }
                if (message.getAck() != 1) {
                    flag = false;
                }
                if (flag) {
                    updateQueueOffset(subscription, pendingQueue, message);
                    this.markPersistOffsetFlag(true);
                    iterator.remove();
                }
            }
        }
    }

    private void updateQueueOffset(Subscription subscription, Queue queue, Message message) {
        String queueName = subscription.toQueueName();
        Map<Queue, QueueOffset> queueOffsetMap = offsetMap.get(queueName);
        if (queueOffsetMap == null || !queueOffsetMap.containsKey(queue)) {
            logger.warn("failed update queue offset,not found queueOffset,{},{},{}", getClientId(), subscription,
                    queue);
            return;
        }
        QueueOffset queueOffset = queueOffsetMap.get(queue);
        queueOffset.setOffset(message.getOffset() + 1);
    }

    public boolean markPersistOffsetFlag(boolean flag) {
        return this.needPersistOffset.compareAndSet(!flag, flag);
    }

    public boolean getPersistOffsetFlag() {
        return needPersistOffset.get();
    }
}

