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

package org.apache.rocketmq.mqtt.common.test.util;

import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
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

  @Test
  public void test() {
    String topic = "test";
    String mqttTopic = "test/mqtt";
    String clientId = "test-mqtt";
    String p2pTopic = Constants.P2P + clientId + Constants.MQTT_TOPIC_DELIMITER;
    String retryTopic = Constants.RETRY + clientId + Constants.MQTT_TOPIC_DELIMITER;

    // test normalizeTopic
    Assert.assertNull(TopicUtils.normalizeTopic(""));
    Assert.assertEquals(topic, TopicUtils.normalizeTopic(topic));
    Assert.assertEquals(mqttTopic + Constants.MQTT_TOPIC_DELIMITER, TopicUtils.normalizeTopic(mqttTopic));

    // test normalizeSecondTopic
    Assert.assertNull(TopicUtils.normalizeSecondTopic(""));
    Assert.assertEquals(Constants.MQTT_TOPIC_DELIMITER + topic + Constants.MQTT_TOPIC_DELIMITER,
            TopicUtils.normalizeSecondTopic(topic));
    Assert.assertEquals(p2pTopic, TopicUtils.normalizeSecondTopic(p2pTopic));

    // test isP2P
    Assert.assertTrue(TopicUtils.isP2P(p2pTopic));

    // test getClientIdFromP2pTopic
    Assert.assertEquals(clientId, TopicUtils.getClientIdFromP2pTopic(p2pTopic));

    // test getClientIdFromRetryTopic
    Assert.assertEquals(clientId, TopicUtils.getClientIdFromRetryTopic(retryTopic));

    // test getP2pTopic, getRetryTopic
    Assert.assertEquals(p2pTopic, TopicUtils.getP2pTopic(clientId));
    Assert.assertEquals(retryTopic, TopicUtils.getRetryTopic(clientId));

    // test isRetryTopic, isP2pTopic
    Assert.assertTrue(TopicUtils.isRetryTopic(retryTopic));
    Assert.assertFalse(TopicUtils.isRetryTopic(topic));
    Assert.assertTrue(TopicUtils.isP2pTopic(p2pTopic));
    Assert.assertFalse(TopicUtils.isP2pTopic(topic));

    // test getP2Peer
    String firstTopic = "first%topic", secondTopic = "/p2p/second";
    String namespace = "namespace";
    MqttTopic mqttTopicObj = new MqttTopic(firstTopic, secondTopic);
    Assert.assertNull(TopicUtils.getP2Peer(new MqttTopic("", ""), namespace));
    Assert.assertNull(TopicUtils.getP2Peer(new MqttTopic(firstTopic, "second"), namespace));
    Assert.assertEquals("namespace%second", TopicUtils.getP2Peer(mqttTopicObj, namespace));

    firstTopic = "first";
    mqttTopicObj = new MqttTopic(firstTopic, secondTopic);
    Assert.assertEquals("second", TopicUtils.getP2Peer(mqttTopicObj, namespace));

    // test encode, decode
    firstTopic = "first"; secondTopic = "/second/";
    String topics = Constants.MQTT_TOPIC_DELIMITER + firstTopic + secondTopic;
    Assert.assertEquals(firstTopic + secondTopic, TopicUtils.encode(firstTopic, secondTopic));
    Assert.assertEquals(firstTopic, TopicUtils.decode(topics).getFirstTopic());
    Assert.assertEquals(secondTopic, TopicUtils.decode(topics).getSecondTopic());

    // test wrapLmp, wrapP2pLmq
    Assert.assertEquals(firstTopic, TopicUtils.wrapLmq(firstTopic, ""));
    Assert.assertEquals(firstTopic + p2pTopic, TopicUtils.wrapLmq(firstTopic, p2pTopic));
    Assert.assertEquals(Constants.P2P + clientId + Constants.MQTT_TOPIC_DELIMITER, TopicUtils.wrapP2pLmq(clientId));
  }
}
