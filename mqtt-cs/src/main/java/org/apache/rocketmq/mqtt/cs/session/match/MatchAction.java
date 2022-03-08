/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.cs.session.match;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


@Component
public class MatchAction {
    private static Logger logger = LoggerFactory.getLogger(MatchAction.class);

    @Resource
    private SessionLoop sessionLoop;

    private Trie<String, Integer> trie = new Trie<>();
    private ConcurrentMap<String, Set<String>> topicCache = new ConcurrentHashMap<>(16);


    public Set<Pair<Session, Subscription>> matchClients(String topic, String namespace) {
        Set<Pair<Session, Subscription>> result = new HashSet<>();
        MqttTopic mqttTopic = TopicUtils.decode(topic);
        String secondTopic = TopicUtils.normalizeSecondTopic(mqttTopic.getSecondTopic());
        if (TopicUtils.isRetryTopic(topic)) {
            String clientId = TopicUtils.getClientIdFromRetryTopic(topic);
            List<Session> sessions = sessionLoop.getSessionList(clientId);
            for (Session session : sessions) {
                result.add(Pair.of(session, Subscription.newRetrySubscription(clientId)));
            }
        } else if (TopicUtils.isP2P(secondTopic)) {
            String clientId = TopicUtils.getP2Peer(mqttTopic, namespace);
            List<Session> sessions = sessionLoop.getSessionList(clientId);
            for (Session session : sessions) {
                result.add(Pair.of(session, Subscription.newP2pSubscription(clientId)));
            }
        } else if(TopicUtils.isP2pTopic(topic)){
            // may be produced by rmq
            String clientId = TopicUtils.getClientIdFromP2pTopic(topic);
            List<Session> sessions = sessionLoop.getSessionList(clientId);
            for (Session session : sessions) {
                result.add(Pair.of(session, Subscription.newP2pSubscription(clientId)));
            }
        } else {
            Set<String> channelIdSet = new HashSet<>();
            synchronized (topicCache) {
                Set<String> precises = topicCache.get(topic);
                if (precises != null && !precises.isEmpty()) {
                    channelIdSet.addAll(precises);
                }
            }
            Map<String, Integer> map = trie.getNode(topic);
            if (map != null && !map.isEmpty()) {
                channelIdSet.addAll(map.keySet());
            }

            for (String channelId : channelIdSet) {
                Session session = sessionLoop.getSession(channelId);
                if (session == null) {
                    continue;
                }
                Set<Subscription> tmp = session.subscriptionSnapshot();
                if (tmp != null && !tmp.isEmpty()) {
                    for (Subscription subscription : tmp) {
                        if (TopicUtils.isMatch(topic, subscription.getTopicFilter())) {
                            result.add(Pair.of(session, subscription));
                        }
                    }
                }
            }
        }
        return result;
    }

    public void addSubscription(Session session, Set<Subscription> subscriptions) {
        String channelId = session.getChannelId();
        if (channelId == null || subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        for (Subscription subscription : subscriptions) {
            if (subscription.isRetry() || subscription.isP2p()) {
                continue;
            }
            String topicFilter = subscription.getTopicFilter();
            boolean isWildCard = TopicUtils.isWildCard(topicFilter);
            if (isWildCard) {
                trie.addNode(topicFilter, subscription.getQos(), channelId);
                continue;
            }

            synchronized (topicCache) {
                if (!topicCache.containsKey(topicFilter)) {
                    topicCache.putIfAbsent(topicFilter, new HashSet<>());
                }
                topicCache.get(topicFilter).add(channelId);
            }
        }
    }

    public void removeSubscription(Session session, Set<Subscription> subscriptions) {
        String channelId = session.getChannelId();
        if (channelId == null || subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        for (Subscription subscription : subscriptions) {
            if (subscription.isRetry() || subscription.isP2p()) {
                continue;
            }
            String topicFilter = subscription.getTopicFilter();
            boolean isWildCard = TopicUtils.isWildCard(topicFilter);
            if (isWildCard) {
                trie.deleteNode(topicFilter, channelId);
                continue;
            }

            synchronized (topicCache) {
                Set<String> channelIdSet = topicCache.get(topicFilter);
                if (channelIdSet != null) {
                    channelIdSet.remove(channelId);
                    if (channelIdSet.isEmpty()) {
                        topicCache.remove(topicFilter);
                    }
                }
            }
        }
    }

}
