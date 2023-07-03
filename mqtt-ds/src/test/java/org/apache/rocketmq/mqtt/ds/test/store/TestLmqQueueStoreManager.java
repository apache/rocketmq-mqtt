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

package org.apache.rocketmq.mqtt.ds.test.store;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
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

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
        message.putUserProperty(Message.extPropertyQoS, "1");
        message.putUserProperty(Message.extPropertyCleanSessionFlag, "test");
        message.putUserProperty(Message.extPropertyClientId, "clientId");

        lmqQueueStoreManager.putMessage(queues, message);
        ArgumentCaptor<org.apache.rocketmq.common.message.Message> argumentCaptor = ArgumentCaptor.forClass(
                org.apache.rocketmq.common.message.Message.class);
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
        when(mqClientInstance.findBrokerAddressInSubscribe(any(), anyLong(), anyBoolean())).thenReturn(
                new FindBrokerResult("test", false));

        lmqQueueStoreManager.pullMessage("test", new Queue(), new QueueOffset(), 1);

        verify(mqClientAPI).pullMessage(any(), any(), anyLong(), any(), any());
    }

    @Test
    public void testToLmqPullRequest() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Queue queue = new Queue(0, "t/t1/", "localhost");
        MessageExt mqMessage = new MessageExt();
        mqMessage.setMsgId("testToLmq");
        mqMessage.setTopic("t/t1/");
        mqMessage.setBody(JSON.toJSONString(Collections.singletonList(new MessageEvent())).getBytes(StandardCharsets.UTF_8));
        Properties properties = new Properties();
        properties.setProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%t%t1%");
        properties.setProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET, "666");
        properties.setProperty(Constants.PROPERTY_MQTT_RETRY_TIMES, "3");
        properties.setProperty(Constants.PROPERTY_MQTT_EXT_DATA, JSON.toJSONString(new HashMap<>()));
        FieldUtils.writeField(mqMessage, "properties", properties, true);

        PullResult pullResult = new org.apache.rocketmq.client.consumer.PullResult(PullStatus.OFFSET_ILLEGAL,
                5, 0, 100, Collections.singletonList(mqMessage));

        Object pullResultObj = MethodUtils.invokeMethod(lmqQueueStoreManager, true, "toLmqPullResult", queue, pullResult);
        org.apache.rocketmq.mqtt.common.model.PullResult mqttPullResult =
                (org.apache.rocketmq.mqtt.common.model.PullResult) pullResultObj;
        Message message = mqttPullResult.getMessageList().iterator().next();

        Assert.assertEquals("testToLmq", message.getMsgId());
        Assert.assertEquals("t/t1/", message.getOriginTopic());
        Assert.assertEquals("t/t1/", message.getFirstTopic());
        Assert.assertEquals(3, message.getRetry());
        Assert.assertEquals(666, message.getOffset());
    }

    @Test
    public void testPullLastMessages() {
        final long pullCount = 100, maxOffset = 5;
        Queue queue = new Queue(0, "t/t1/", "localhost");
        LmqQueueStoreManager spyLmqQueueStoreManager = spy(lmqQueueStoreManager);
        CompletableFuture<Long> maxOffsetFuture = new CompletableFuture<>();
        maxOffsetFuture.complete(maxOffset);
        doReturn(maxOffsetFuture).when(spyLmqQueueStoreManager).queryQueueMaxOffset(queue);

        QueueOffset verifyOffset = new QueueOffset();
        verifyOffset.setOffset(0);
        doReturn(null).when(spyLmqQueueStoreManager).pullMessage(eq("test"), eq(queue), eq(verifyOffset), eq(pullCount));

        spyLmqQueueStoreManager.pullLastMessages("test", queue, pullCount);
        verify(spyLmqQueueStoreManager).pullMessage(eq("test"), eq(queue), eq(verifyOffset), eq(pullCount));
    }

    @Test
    public void testQueryQueueMaxOffset() throws Exception {
        Queue queue = new Queue(0, "t/t1/", "localhost");
        final long maxOffset = 5;
        DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = mock(DefaultMQPullConsumerImpl.class);
        when(defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(defaultMQPullConsumerImpl);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        when(defaultMQPullConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        when(rebalanceImpl.getmQClientFactory()).thenReturn(mqClientInstance);
        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPI);
        when(mqClientInstance.findBrokerAddressInPublish(anyString())).thenReturn(
                String.valueOf(new FindBrokerResult("test", false)));
        when(mqClientAPI.getMaxOffset(anyString(), any(MessageQueue.class), anyLong())).thenReturn(maxOffset);

        CompletableFuture<Long> queryOffsetFuture = lmqQueueStoreManager.queryQueueMaxOffset(queue);
        verify(mqClientInstance, times(0)).updateTopicRouteInfoFromNameServer(any());
        Assert.assertEquals(maxOffset, queryOffsetFuture.get().longValue());
    }

}
