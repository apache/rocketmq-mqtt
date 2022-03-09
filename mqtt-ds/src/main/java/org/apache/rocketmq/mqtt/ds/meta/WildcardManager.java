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

package org.apache.rocketmq.mqtt.ds.meta;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.StatUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class WildcardManager {
    private static Logger logger = LoggerFactory.getLogger(WildcardManager.class);
    private Map<String, Trie<String, Integer>> wildCardTrie = new ConcurrentHashMap<>();
    private ScheduledThreadPoolExecutor scheduler;

    @Resource
    private MetaPersistManager metaPersistManager;

    @PostConstruct
    public void init() {
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("loadWildcard_thread_"));
        scheduler.scheduleWithFixedDelay(() -> refreshLoadWildcard(), 0, 5, TimeUnit.SECONDS);
    }

    private void refreshLoadWildcard() {
        try {
            Set<String> topics = metaPersistManager.getAllFirstTopics();
            if (topics == null) {
                return;
            }
            topics.forEach(firstTopic -> refreshWildcards(firstTopic));
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void refreshWildcards(String firstTopic) {
        Trie<String, Integer> trie = new Trie<>();
        Trie<String, Integer> old = wildCardTrie.putIfAbsent(firstTopic, trie);
        if (old != null) {
            trie = old;
        }
        Set<String> wildcards = metaPersistManager.getWildcards(firstTopic);
        if (wildcards != null && !wildcards.isEmpty()) {
            for (String each : wildcards) {
                trie.addNode(each, 0, "");
            }
        }
        //clean unused key
        Trie<String, Integer> finalTrie = trie;
        trie.traverseAll((path, nodeKey) -> {
            if (!wildcards.contains(path)) {
                finalTrie.deleteNode(path, nodeKey);
            }
        });
    }

    public Set<String> matchQueueSetByMsgTopic(String pubTopic, String namespace) {
        Set<String> queueNames = new HashSet<>();
        if (StringUtils.isBlank(pubTopic)) {
            return queueNames;
        }
        MqttTopic mqttTopic = TopicUtils.decode(pubTopic);
        String secondTopic = TopicUtils.normalizeSecondTopic(mqttTopic.getSecondTopic());
        if (TopicUtils.isP2P(secondTopic)) {
            String p2Peer = TopicUtils.getP2Peer(mqttTopic, namespace);
            queueNames.add(TopicUtils.getP2pTopic(p2Peer));
        } else {
            queueNames.add(pubTopic);
            Set<String> wildcards = matchWildcards(pubTopic);
            if (wildcards != null && !wildcards.isEmpty()) {
                for (String wildcard : wildcards) {
                    queueNames.add(wildcard);
                }
            }
        }
        return queueNames;
    }

    private Set<String> matchWildcards(String topic) {
        long start = System.currentTimeMillis();
        try {
            MqttTopic mqttTopic = TopicUtils.decode(topic);
            Trie<String, Integer> trie = wildCardTrie.get(mqttTopic.getFirstTopic());
            if (trie == null) {
                return new HashSet<>();
            }
            return trie.getNodePath(topic);
        } finally {
            StatUtil.addInvoke("MatchWildcards", System.currentTimeMillis() - start);
        }
    }

}
