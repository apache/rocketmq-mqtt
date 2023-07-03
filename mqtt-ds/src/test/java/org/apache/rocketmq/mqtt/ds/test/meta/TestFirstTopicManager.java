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

package org.apache.rocketmq.mqtt.ds.test.meta;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestFirstTopicManager {

    @Mock
    private MetaPersistManager metaPersistManager;

    @Mock
    private ServiceConf serviceConf;

    @Mock
    private DefaultMQAdminExt defaultMQAdminExt;

    private FirstTopicManager firstTopicManager;

    @Before
    public void setUp() throws Exception {
        firstTopicManager = new FirstTopicManager();
        FieldUtils.writeDeclaredField(firstTopicManager, "defaultMQAdminExt", defaultMQAdminExt, true);
        FieldUtils.writeDeclaredField(firstTopicManager, "serviceConf", serviceConf, true);
        FieldUtils.writeDeclaredField(firstTopicManager, "metaPersistManager", metaPersistManager, true);
    }

    @Test
    public void testInit() throws MQClientException, RemotingException, InterruptedException {
        String p2pClientTopic = "tm/p2p/test";
        when(serviceConf.getClientRetryTopic()).thenReturn(null);
        when(serviceConf.getClientP2pTopic()).thenReturn(p2pClientTopic);
        when(metaPersistManager.getAllFirstTopics()).thenReturn(new HashSet<>());
        when(defaultMQAdminExt.examineTopicRouteInfo(p2pClientTopic)).thenReturn(null);
        FirstTopicManager spyFirstTopicManger = spy(firstTopicManager);
        doNothing().when(spyFirstTopicManger).initMQAdminExt();

        spyFirstTopicManger.init();
        Thread.sleep(100);

        Assert.assertTrue(spyFirstTopicManger.getBrokerAddressMap(p2pClientTopic).isEmpty());
        Assert.assertTrue(spyFirstTopicManger.getReadableBrokers(p2pClientTopic).isEmpty());
    }

    @Test
    public void test() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException {
        Cache<String, TopicRouteData> topicExistCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.MINUTES).build();
        Cache<String, Object> topicNotExistCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(1, TimeUnit.MINUTES).build();

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

        Assert.assertNotNull(topicExistCache.getIfPresent("test"));
        Assert.assertEquals("test", firstTopicManager.getBrokerAddressMap("test").keySet().iterator().next());
        Assert.assertEquals("test", firstTopicManager.getReadableBrokers("test").iterator().next());
    }

    @Test
    public void notExistTopicRoute() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException, InvocationTargetException, NoSuchMethodException {
        when(defaultMQAdminExt.examineTopicRouteInfo(anyString())).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));
        Map<String, Set<String>> readableBrokers = mock(HashMap.class);
        FieldUtils.writeDeclaredField(firstTopicManager, "readableBrokers", readableBrokers, true);
        MethodUtils.invokeMethod(firstTopicManager, true, "updateTopicRoute", "test");
        verify(readableBrokers).remove(eq("test"));
    }

}
