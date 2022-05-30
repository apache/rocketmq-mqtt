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


import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Component
public class InFlyCache {

    @Resource
    private MqttMsgId mqttMsgId;

    private ConcurrentMap<String, Set<Integer>> pubCache = new ConcurrentHashMap<>(128);
    private PendingDownCache pendingDownCache = new PendingDownCache();

    public void cleanResource(String clientId, String channelId) {
        pubCache.remove(channelId);
        pendingDownCache.clear(clientId, channelId);
    }

    public enum CacheType {
        PUB,
    }

    private ConcurrentMap<String, Set<Integer>> whichCache(CacheType cacheType) {
        switch (cacheType) {
            case PUB:
                return pubCache;
            default:
                throw new RuntimeException("invalid cache type");
        }
    }

    public void put(CacheType cacheType, String channelId, int mqttMsgId) {
        ConcurrentMap<String, Set<Integer>> cache = whichCache(cacheType);
        cache.putIfAbsent(channelId, new HashSet<>());
        Set<Integer> idCache = cache.get(channelId);
        if (idCache == null) {
            return;
        }
        synchronized (idCache) {
            idCache.add(mqttMsgId);
        }
    }

    public boolean contains(CacheType cacheType, String channelId, int mqttMsgId) {
        ConcurrentMap<String, Set<Integer>> cache = whichCache(cacheType);
        Set<Integer> idCache = cache.get(channelId);
        if (idCache == null) {
            return false;
        }
        synchronized (idCache) {
            return idCache.contains(mqttMsgId);
        }
    }

    public void remove(CacheType cacheType, String channelId, int mqttMsgId) {
        ConcurrentMap<String, Set<Integer>> cache = whichCache(cacheType);
        Set<Integer> idCache = cache.get(channelId);
        if (idCache == null) {
            return;
        }
        synchronized (idCache) {
            idCache.remove(mqttMsgId);
            if (idCache.isEmpty()) {
                cache.remove(channelId);
            }
        }
    }

    public PendingDownCache getPendingDownCache() {
        return pendingDownCache;
    }

    public class PendingDownCache {
        private ConcurrentMap<String, Map<Integer, PendingDown>> cache = new ConcurrentHashMap<>(128);

        public PendingDown put(String channelId, int mqttMsgId, Subscription subscription, Queue queue,
                               Message message) {
            PendingDown pendingDown = new PendingDown();
            pendingDown.setSubscription(subscription);
            pendingDown.setQueue(queue);
            pendingDown.setSeqId(message.getOffset());
            cache.putIfAbsent(channelId, new ConcurrentHashMap<>(16));
            cache.get(channelId).put(mqttMsgId, pendingDown);
            return pendingDown;
        }

        public Map<Integer, PendingDown> all(String channelId) {
            if (StringUtils.isBlank(channelId)) {
                return null;
            }
            return cache.get(channelId);
        }

        public PendingDown remove(String channelId, int mqttMsgId) {
            Map<Integer, PendingDown> map = cache.get(channelId);
            if (map != null) {
                return map.remove(mqttMsgId);
            }
            return null;
        }

        public PendingDown get(String channelId, int mqttMsgId) {
            Map<Integer, PendingDown> map = cache.get(channelId);
            if (map != null) {
                return map.get(mqttMsgId);
            }
            return null;
        }

        public void clear(String clientId, String channelId) {
            Map<Integer, PendingDown> pendingDownMap = cache.remove(channelId);
            if (clientId != null && pendingDownMap != null) {
                pendingDownMap.forEach((mqttId, pendingDown) -> mqttMsgId.releaseId(mqttId, clientId));
            }
        }
    }

    public class PendingDown {
        private Subscription subscription;
        private Queue queue;
        private long seqId;
        private long t = System.currentTimeMillis();

        public Subscription getSubscription() {
            return subscription;
        }

        public void setSubscription(Subscription subscription) {
            this.subscription = subscription;
        }

        public Queue getQueue() {
            return queue;
        }

        public void setQueue(Queue queue) {
            this.queue = queue;
        }

        public long getSeqId() {
            return seqId;
        }

        public void setSeqId(long seqId) {
            this.seqId = seqId;
        }

        public long getT() {
            return t;
        }
    }

}
