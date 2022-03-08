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

package org.apache.rocketmq.mqtt.ds.test;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestFirstTopicManager {

    @Mock
    private MetaPersistManager metaPersistManager;

    @Mock
    private ServiceConf serviceConf;

    @Test
    public void test() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException {
        FirstTopicManager firstTopicManager = new FirstTopicManager();
        DefaultMQAdminExt defaultMQAdminExt = mock(DefaultMQAdminExt.class);

        Cache<String, TopicRouteData> topicExistCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.MINUTES).build();
        Cache<String, Object> topicNotExistCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.MINUTES).build();

        FieldUtils.writeDeclaredField(firstTopicManager, "defaultMQAdminExt", defaultMQAdminExt, true);
        FieldUtils.writeDeclaredField(firstTopicManager, "topicExistCache", topicExistCache, true);
        FieldUtils.writeDeclaredField(firstTopicManager, "topicNotExistCache", topicNotExistCache, true);

        TopicRouteData topicRouteData = new TopicRouteData();

        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "test");
        BrokerData brokerData = new BrokerData("test", "test", brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);

        QueueData queueData = new QueueData();
        queueData.setPerm(6);
        queueData.setReadQueueNums(1);
        queueData.setBrokerName("test");
        List<QueueData> queueDatas = new ArrayList<>();
        queueDatas.add(queueData);
        topicRouteData.setQueueDatas(queueDatas);

        when(defaultMQAdminExt.examineTopicRouteInfo(any())).thenReturn(topicRouteData);
        firstTopicManager.checkFirstTopicIfCreated("test");

        Assert.assertFalse(topicExistCache.getIfPresent("test") == null);
        Assert.assertTrue("test".equals(firstTopicManager.getBrokerAddressMap("test").keySet().iterator().next()));
        Assert.assertTrue("test".equals(firstTopicManager.getReadableBrokers("test").iterator().next()));
    }

}
