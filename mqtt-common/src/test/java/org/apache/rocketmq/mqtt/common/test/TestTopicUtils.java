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

package org.apache.rocketmq.mqtt.common.test;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestTopicUtils {

  @Test
  public void testTopicMatch() {
    String topic = "t/t1/t2/";
    String topicFilter = "t/t1/t2/";
    Assert.assertTrue(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/#/";
    Assert.assertTrue(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/t1/+/";
    Assert.assertTrue(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/+/#/";
    Assert.assertTrue(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/#/t2/";
    Assert.assertFalse(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/#/+/";
    Assert.assertFalse(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/t1/t2/t3/";
    Assert.assertFalse(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/t1/#/t3/";
    Assert.assertFalse(TopicUtils.isMatch(topic, topicFilter));

    topicFilter = "t/t1#/";
    Assert.assertFalse(TopicUtils.isMatch(topic, topicFilter));


  }
}
