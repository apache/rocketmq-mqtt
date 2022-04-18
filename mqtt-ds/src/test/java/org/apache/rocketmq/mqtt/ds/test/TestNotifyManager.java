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
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.common.model.RpcCode;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.notify.NotifyManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestNotifyManager {

    @Mock
    private MetaPersistManager metaPersistManager;

    @Mock
    private FirstTopicManager firstTopicManager;

    @Mock
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Mock
    private NettyRemotingClient remotingClient;

    @Mock
    private ServiceConf serviceConf;

    @Test
    public void test() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException,
            MQClientException, RemotingException, InterruptedException, MQBrokerException {
        NotifyManager notifyManager = new NotifyManager();
        FieldUtils.writeDeclaredField(notifyManager, "metaPersistManager", metaPersistManager, true);
        FieldUtils.writeDeclaredField(notifyManager, "firstTopicManager", firstTopicManager, true);
        FieldUtils.writeDeclaredField(notifyManager, "defaultMQPushConsumer", defaultMQPushConsumer, true);
        FieldUtils.writeDeclaredField(notifyManager, "remotingClient", remotingClient, true);
        FieldUtils.writeDeclaredField(notifyManager, "serviceConf", serviceConf, true);

        when(metaPersistManager.getAllFirstTopics()).thenReturn(new HashSet<>(Arrays.asList("test")));

        MethodUtils.invokeMethod(notifyManager, true, "refresh");
        verify(defaultMQPushConsumer).subscribe(any(), anyString());

        when(metaPersistManager.getConnectNodeSet()).thenReturn(new HashSet<>(Arrays.asList("test")));
        RemotingCommand response = mock(RemotingCommand.class);
        when(response.getCode()).thenReturn(RpcCode.SUCCESS);
        when(remotingClient.invokeSync(any(), any(), anyLong())).thenReturn(response);
        notifyManager.notifyMessage(new HashSet<>(Arrays.asList(new MessageEvent())));
        verify(remotingClient).invokeSync(any(), any(), anyLong());
    }

    @Test
    public void testJsonByte() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Set<MessageEvent> messageEvents=new HashSet<>();
        for (int i=0;i<10;i++) {
            MessageEvent messageEvent = new MessageEvent();
            messageEvent.setBrokerName("testBroker"+i);
            messageEvent.setPubTopic("testTopic"+i);
            messageEvent.setNamespace("testSpace"+i);
            messageEvents.add(messageEvent);
        }
        NotifyManager notifyManager = new NotifyManager();
        RemotingCommand remotingCommand = (RemotingCommand) MethodUtils.invokeMethod(notifyManager,  true, "createMsgEventCommand", messageEvents);
        byte[] bytes = JSONObject.toJSONString(messageEvents).getBytes(StandardCharsets.UTF_8);
        Assert.assertArrayEquals(remotingCommand.getBody(), bytes);
    }

}
