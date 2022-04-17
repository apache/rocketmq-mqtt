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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.store.LmqQueueStoreManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestLmqQueueStoreManager {

    @Mock
    private FirstTopicManager firstTopicManager;

    @Mock
    private ServiceConf serviceConf;

    @Mock
    private PullAPIWrapper pullAPIWrapper;

    @Mock
    private DefaultMQPullConsumer defaultMQPullConsumer;

    @Mock
    private DefaultMQProducer defaultMQProducer;

    private LmqQueueStoreManager lmqQueueStoreManager;

    @Before
    public void before() throws IllegalAccessException {
        lmqQueueStoreManager = new LmqQueueStoreManager();
        FieldUtils.writeDeclaredField(lmqQueueStoreManager, "firstTopicManager", firstTopicManager, true);
        FieldUtils.writeDeclaredField(lmqQueueStoreManager, "serviceConf", serviceConf, true);
        FieldUtils.writeDeclaredField(lmqQueueStoreManager, "pullAPIWrapper", pullAPIWrapper, true);
        FieldUtils.writeDeclaredField(lmqQueueStoreManager, "defaultMQPullConsumer", defaultMQPullConsumer, true);
        FieldUtils.writeDeclaredField(lmqQueueStoreManager, "defaultMQProducer", defaultMQProducer, true);
    }

    @Test
    public void testPutMessage() throws RemotingException, InterruptedException, MQClientException {
        Set<String> queues = new HashSet<>(Arrays.asList("test"));
        Message message = new Message();
        message.setOriginTopic("test");
        lmqQueueStoreManager.putMessage(queues, message);
        ArgumentCaptor<org.apache.rocketmq.common.message.Message> argumentCaptor = ArgumentCaptor.forClass(org.apache.rocketmq.common.message.Message.class);
        verify(defaultMQProducer).send(argumentCaptor.capture(), any(SendCallback.class));
        Assert.assertTrue(null != argumentCaptor.getValue().getUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH));
    }

    @Test
    public void testPullMessage() throws MQBrokerException, RemotingException, InterruptedException {
        DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = mock(DefaultMQPullConsumerImpl.class);
        when(defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(defaultMQPullConsumerImpl);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        when(defaultMQPullConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        when(rebalanceImpl.getmQClientFactory()).thenReturn(mqClientInstance);
        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPI);
        when(mqClientInstance.findBrokerAddressInSubscribe(any(), anyLong(), anyBoolean())).thenReturn(new FindBrokerResult("test", false));

        lmqQueueStoreManager.pullMessage("test", new Queue(), new QueueOffset(), 1);

        verify(mqClientAPI).pullMessage(any(), any(), anyLong(), any(), any());
    }

}
