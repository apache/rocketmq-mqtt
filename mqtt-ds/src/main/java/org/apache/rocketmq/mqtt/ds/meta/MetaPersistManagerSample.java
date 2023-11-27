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
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.mq.MqFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A Sample For Meta Manager, Persisted In namesrv KV Config
 */
public class MetaPersistManagerSample implements MetaPersistManager {
    private static Logger logger = LoggerFactory.getLogger(MetaPersistManagerSample.class);
    private volatile Map<String, Set<String>> wildcardCache = new ConcurrentHashMap<>();
    private volatile Set<String> firstTopics = new HashSet<>();
    private volatile Set<String> connectNodeSet = new HashSet<>();
    private DefaultMQAdminExt defaultMQAdminExt;
    private ScheduledThreadPoolExecutor scheduler;
    private static final String RMQ_NAMESPACE = "LMQ";
    private static final String KEY_LMQ_ALL_FIRST_TOPICS = "ALL_FIRST_TOPICS";
    private static final String VALUE_SPLITTER = ",";
    private static final String NODE_ADDR_SPLITTER = ":";
    private String dispatcherConsumerGroup = MixAll.CID_RMQ_SYS_PREFIX + "mqtt_event";

    @Resource
    private ServiceConf serviceConf;

    public void init() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        defaultMQAdminExt = MqFactory.buildDefaultMQAdminExt("MetaLoad", serviceConf.getProperties());
        defaultMQAdminExt.start();
        refreshMeta();
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("refreshMeta"));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshMeta();
            } catch (Throwable t) {
                logger.error("", t);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void refreshMeta() throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        String value = defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE, KEY_LMQ_ALL_FIRST_TOPICS);
        if (value == null) {
            return;
        }
        String[] topics = value.split(VALUE_SPLITTER);
        Set<String> tmpFirstTopics = new HashSet<>();
        Map<String, Set<String>> tmpWildcardCache = new ConcurrentHashMap<>();
        for (String topic : topics) {
            tmpFirstTopics.add(topic);
            try {
                String wildcardValue = defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE, topic);
                String[] wildcards = wildcardValue.split(VALUE_SPLITTER);
                Set<String> tmpWildcards = new HashSet<>();
                for (String wildcard : wildcards) {
                    tmpWildcards.add(TopicUtils.normalizeTopic(wildcard));
                }
                tmpWildcardCache.put(topic, tmpWildcards);
            } catch (MQClientException e) {
                if (ResponseCode.QUERY_NOT_FOUND == e.getResponseCode()) {
                    continue;
                }
                throw e;
            }
        }
        firstTopics = tmpFirstTopics;
        wildcardCache = tmpWildcardCache;

        ConsumerConnection consumerConn = defaultMQAdminExt.examineConsumerConnectionInfo(dispatcherConsumerGroup);
        if (CollectionUtils.isEmpty(consumerConn.getConnectionSet())) {
            return;
        }
        Set<String> clientIpSet = new HashSet<>();
        consumerConn.getConnectionSet().forEach(connection -> {
            clientIpSet.add(connection.getClientAddr().split(NODE_ADDR_SPLITTER)[0]);
        });
        connectNodeSet = clientIpSet;
    }

    @Override
    public Set<String> getWildcards(String firstTopic) {
        return wildcardCache.get(firstTopic);
    }

    @Override
    public Set<String> getAllFirstTopics() {
        return firstTopics;
    }

    @Override
    public Set<String> getConnectNodeSet() {
        return connectNodeSet;
    }

}
