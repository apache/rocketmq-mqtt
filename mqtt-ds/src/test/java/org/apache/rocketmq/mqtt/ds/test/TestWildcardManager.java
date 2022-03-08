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
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestWildcardManager {

    @Test
    public void test() throws IllegalAccessException, InterruptedException {
        WildcardManager wildcardManager = new WildcardManager();
        MetaPersistManager metaPersistManager = mock(MetaPersistManager.class);
        FieldUtils.writeDeclaredField(wildcardManager, "metaPersistManager", metaPersistManager, true);

        when(metaPersistManager.getAllFirstTopics()).thenReturn(new HashSet<>(Arrays.asList("test")));
        when(metaPersistManager.getWildcards(any())).thenReturn(new HashSet<>(Arrays.asList(TopicUtils.normalizeTopic("test/+"))));

        wildcardManager.init();
        Thread.sleep(1000L);

        Set<String> set =  wildcardManager.matchQueueSetByMsgTopic(TopicUtils.normalizeTopic("test/a"),"");
        Assert.assertTrue(set.contains(TopicUtils.normalizeTopic("test/+")));
        Assert.assertTrue(set.contains(TopicUtils.normalizeTopic("test/a")));
    }

    @Test
    public void testForP2P() throws IllegalAccessException, InterruptedException {
        WildcardManager wildcardManager = new WildcardManager();
        MetaPersistManager metaPersistManager = mock(MetaPersistManager.class);
        FieldUtils.writeDeclaredField(wildcardManager, "metaPersistManager", metaPersistManager, true);

        when(metaPersistManager.getAllFirstTopics()).thenReturn(new HashSet<>(Arrays.asList("test")));
        when(metaPersistManager.getWildcards(any())).thenReturn(new HashSet<>(Arrays.asList(TopicUtils.normalizeTopic("test/+"))));

        wildcardManager.init();
        Thread.sleep(1000L);

        Set<String> set =  wildcardManager.matchQueueSetByMsgTopic(TopicUtils.normalizeTopic("test/p2p/GID_sdasa@@@2222"),"");
        Assert.assertTrue(set.contains(TopicUtils.normalizeTopic("/p2p/GID_sdasa@@@2222/")));
    }

}
