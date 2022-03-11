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
