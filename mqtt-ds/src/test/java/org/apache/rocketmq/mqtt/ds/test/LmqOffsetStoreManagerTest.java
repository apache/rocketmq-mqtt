package org.apache.rocketmq.mqtt.ds.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.store.LmqOffsetStoreManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author tianliuliu
 * @date 2022-03-14 11:50
 */
@RunWith(MockitoJUnitRunner.class)
public class LmqOffsetStoreManagerTest {
    @Mock
    private FirstTopicManager firstTopicManager;

    @Mock
    private ServiceConf serviceConf;

    @Mock
    private DefaultMQPullConsumer defaultMQPullConsumer;

    private LmqOffsetStoreManager lmqOffsetStoreManager;

    @Before
    public void before() throws IllegalAccessException {
        lmqOffsetStoreManager = new LmqOffsetStoreManager();
        FieldUtils.writeDeclaredField(lmqOffsetStoreManager, "firstTopicManager", firstTopicManager, true);
        FieldUtils.writeDeclaredField(lmqOffsetStoreManager, "serviceConf", serviceConf, true);
        FieldUtils.writeDeclaredField(lmqOffsetStoreManager, "defaultMQPullConsumer", defaultMQPullConsumer, true);
    }

    @Test
    public void testSave() throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        Subscription subscription = new Subscription("t/t1/t2");
        String clientId = "gid_test@@@qwewqee";
        Map<Queue, QueueOffset> queueMap = new HashMap<>();
        queueMap.put(new Queue(0, "t/t1/t2", "test"), new QueueOffset(0));
        Map<Subscription, Map<Queue, QueueOffset>> offsetMap = new HashMap<>();
        offsetMap.put(subscription, queueMap);

        Map<String, String> map = new HashMap();
        map.put("test", "127.0.0.1:10911");
        when(firstTopicManager.getBrokerAddressMap(any())).thenReturn(map);

        DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = mock(DefaultMQPullConsumerImpl.class);
        when(defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(defaultMQPullConsumerImpl);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        when(defaultMQPullConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        when(rebalanceImpl.getmQClientFactory()).thenReturn(mqClientInstance);

        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPI);

        lmqOffsetStoreManager.save(clientId, offsetMap);

        verify(mqClientAPI).updateConsumerOffset(any(), any(), anyLong());
    }

    @Test
    public void testGetOffset() throws RemotingException, InterruptedException, MQBrokerException {
        Subscription subscription = new Subscription("t/t1/t2");
        String clientId = "gid_test@@@qwewqee";
        Queue queue = new Queue(0, "t/t1/t2", "test");

        Map<String, String> map = new HashMap();
        map.put("test", "127.0.0.1:10911");
        when(firstTopicManager.getBrokerAddressMap(any())).thenReturn(map);

        DefaultMQPullConsumerImpl defaultMQPullConsumerImpl = mock(DefaultMQPullConsumerImpl.class);
        when(defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(defaultMQPullConsumerImpl);
        RebalanceImpl rebalanceImpl = mock(RebalanceImpl.class);
        when(defaultMQPullConsumerImpl.getRebalanceImpl()).thenReturn(rebalanceImpl);
        MQClientInstance mqClientInstance = mock(MQClientInstance.class);
        when(rebalanceImpl.getmQClientFactory()).thenReturn(mqClientInstance);

        MQClientAPIImpl mqClientAPI = mock(MQClientAPIImpl.class);
        when(mqClientInstance.getMQClientAPIImpl()).thenReturn(mqClientAPI);

        when(mqClientAPI.queryConsumerOffset(any(), any(), anyLong())).thenReturn(10L);

        CompletableFuture<Map<Queue, QueueOffset>> offset = lmqOffsetStoreManager.getOffset(clientId, subscription);

        offset.whenComplete((offsetMap, throwable) -> {
            long offset1 = offsetMap.get(queue).getOffset();
            Assert.assertTrue(offset1 == 10L);
        });
        verify(mqClientAPI).queryConsumerOffset(any(), any(), anyLong());

    }
}