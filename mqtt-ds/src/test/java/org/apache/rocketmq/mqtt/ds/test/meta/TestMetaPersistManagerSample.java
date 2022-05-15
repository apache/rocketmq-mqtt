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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.MetaPersistManagerSample;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMetaPersistManagerSample {
    private static final String RMQ_NAMESPACE = "LMQ";
    private static final String KEY_LMQ_ALL_FIRST_TOPICS = "ALL_FIRST_TOPICS";
    private static final String KEY_LMQ_CONNECT_NODES = "LMQ_CONNECT_NODES";

    @Test
    public void refreshMeta() throws IllegalAccessException, RemotingException, InterruptedException, MQClientException, InvocationTargetException, NoSuchMethodException {
        MetaPersistManagerSample metaPersistManagerSample = new MetaPersistManagerSample();
        DefaultMQAdminExt defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        FieldUtils.writeDeclaredField(metaPersistManagerSample, "defaultMQAdminExt", defaultMQAdminExt, true);
        String firstTopic = "test";
        String wildcards = "test/2/#";
        String node = "localhost";
        when(defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE,KEY_LMQ_ALL_FIRST_TOPICS)).thenReturn(firstTopic);
        when(defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE,firstTopic)).thenReturn(wildcards);
        when(defaultMQAdminExt.getKVConfig(RMQ_NAMESPACE,KEY_LMQ_CONNECT_NODES)).thenReturn(node);
        MethodUtils.invokeMethod(metaPersistManagerSample, true, "refreshMeta");
        Assert.assertTrue(firstTopic.equals(metaPersistManagerSample.getAllFirstTopics().iterator().next()));
        Assert.assertTrue(TopicUtils.normalizeTopic(wildcards).equals(metaPersistManagerSample.getWildcards(firstTopic).iterator().next()));
        Assert.assertTrue(node.equals(metaPersistManagerSample.getConnectNodeSet().iterator().next()));
    }
}
