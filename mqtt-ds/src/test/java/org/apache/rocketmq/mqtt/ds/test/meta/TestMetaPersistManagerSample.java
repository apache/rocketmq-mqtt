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

package org.apache.rocketmq.mqtt.ds.test.meta;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.MetaPersistManagerSample;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMetaPersistManagerSample {

    private static final String RMQ_NAMESPACE = "LMQ";

    private static final String KEY_LMQ_ALL_FIRST_TOPICS = "ALL_FIRST_TOPICS";

    @Test
    public void refreshMeta() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException, InvocationTargetException, NoSuchMethodException, MQBrokerException {
        MetaPersistManagerSample metaPersistManagerSample = new MetaPersistManagerSample();
        DefaultMQAdminExt defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        FieldUtils.writeDeclaredField(metaPersistManagerSample, "defaultMQAdminExt", defaultMQAdminExt, true);
        String firstTopic = "test";
        String wildcards = "test/2/#";
        when(defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE,KEY_LMQ_ALL_FIRST_TOPICS)).thenReturn(firstTopic);
        when(defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE,firstTopic)).thenReturn(wildcards);
        when(defaultMQAdminExt.examineConsumerConnectionInfo("CID_RMQ_SYS_mqtt_event")).thenReturn(createConsumerConnection());
        MethodUtils.invokeMethod(metaPersistManagerSample, true, "refreshMeta");
        assertEquals(firstTopic, metaPersistManagerSample.getAllFirstTopics().iterator().next());
        assertEquals(TopicUtils.normalizeTopic(wildcards), metaPersistManagerSample.getWildcards(firstTopic).iterator().next());
        assertEquals("localhost", metaPersistManagerSample.getConnectNodeSet().iterator().next());
    }

    private ConsumerConnection createConsumerConnection() {
        ConsumerConnection result = new ConsumerConnection();
        Connection connection = new Connection();
        connection.setClientAddr("localhost");
        result.getConnectionSet().add(connection);
        return result;
    }
}
