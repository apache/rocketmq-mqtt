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

package org.apache.rocketmq.mqtt.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;


public class TopicUtils {

    /**
     * t1/t2/t3/
     *
     * @param topic
     * @return
     */
    public static String normalizeTopic(String topic) {
        if (StringUtils.isBlank(topic)) {
            return null;
        }
        if (!topic.contains(Constants.MQTT_TOPIC_DELIMITER)) {
            return topic;
        }
        if (!topic.endsWith(Constants.MQTT_TOPIC_DELIMITER)) {
            return topic + Constants.MQTT_TOPIC_DELIMITER;
        }
        return topic;
    }

    /**
     * /t2/t3/t4/
     *
     * @param secondTopic
     * @return
     */
    public static String normalizeSecondTopic(String secondTopic) {
        if (StringUtils.isBlank(secondTopic)) {
            return null;
        }
        if (!secondTopic.startsWith(Constants.MQTT_TOPIC_DELIMITER)) {
            secondTopic = Constants.MQTT_TOPIC_DELIMITER + secondTopic;
        }
        if (!secondTopic.endsWith(Constants.MQTT_TOPIC_DELIMITER)) {
            return secondTopic + Constants.MQTT_TOPIC_DELIMITER;
        }
        return secondTopic;
    }

    public static boolean isP2P(String secondTopic) {
        return secondTopic != null && secondTopic.startsWith(Constants.P2P);
    }

    public static String getClientIdFromP2pTopic(String p2pTopic) {
        String tmp = p2pTopic.substring(Constants.P2P.length());
        return tmp.substring(0, tmp.length() - 1);
    }

    public static String getClientIdFromRetryTopic(String retryTopic) {
        String tmp = retryTopic.substring(Constants.RETRY.length());
        return tmp.substring(0, tmp.length() - 1);
    }

    public static String getP2pTopic(String clientId) {
        return normalizeTopic(Constants.P2P + clientId + Constants.MQTT_TOPIC_DELIMITER);
    }

    public static String getRetryTopic(String clientId) {
        return normalizeTopic(Constants.RETRY + clientId + Constants.MQTT_TOPIC_DELIMITER);
    }

    public static boolean isRetryTopic(String topic) {
        return topic != null && topic.startsWith(Constants.RETRY);
    }

    public static boolean isP2pTopic(String topic) {
        return topic != null && topic.startsWith(Constants.P2P);
    }

    public static String getP2Peer(MqttTopic mqttTopic, String namespace) {
        if (mqttTopic.getSecondTopic() == null || mqttTopic.getFirstTopic() == null) {
            return null;
        }
        if (!isP2P(mqttTopic.getSecondTopic())) {
            return null;
        }
        if (mqttTopic.getFirstTopic().contains(Constants.NAMESPACE_SPLITER) && StringUtils.isNotBlank(namespace)) {
            return StringUtils.join(namespace, Constants.NAMESPACE_SPLITER, mqttTopic.getSecondTopic().split(Constants.MQTT_TOPIC_DELIMITER)[2]);
        }
        return mqttTopic.getSecondTopic().split(Constants.MQTT_TOPIC_DELIMITER)[2];
    }

    public static String encode(String topic, String secondTopic) {
        if (secondTopic != null && secondTopic.length() > 1) {
            return topic + secondTopic;
        }
        return topic;
    }

    public static MqttTopic decode(String topics) {
        if (topics.startsWith(Constants.MQTT_TOPIC_DELIMITER)) {
            topics = topics.substring(1);
        }
        String topic;
        String secondTopic = null;
        int index = topics.indexOf(Constants.MQTT_TOPIC_DELIMITER, 1);
        if (index > 0) {
            topic = topics.substring(0, index);
            secondTopic = topics.substring(index);
        } else {
            topic = topics;
        }
        return new MqttTopic(topic, secondTopic);
    }

    public static boolean isWildCard(String topicFilter) {
        return topicFilter != null &&
                (topicFilter.contains(Constants.NUMBER_SIGN) || topicFilter.contains(Constants.PLUS_SIGN));
    }

    @SuppressWarnings("checkstyle:UnnecessaryParentheses")
    public static boolean isMatch(String topic, String topicFilter) {
        if (topic.equals(topicFilter)) {
            return true;
        }
        if (!isWildCard(topicFilter)) {
            return false;
        }

        String[] subscribeTopics = topicFilter.split(Constants.MQTT_TOPIC_DELIMITER);
        String[] messageTopics = topic.split(Constants.MQTT_TOPIC_DELIMITER);
        int targetTopicLength = messageTopics.length;
        int sourceTopicLength = subscribeTopics.length;
        int minTopicLength = Math.min(targetTopicLength, sourceTopicLength);

        for (int i = 0; i < minTopicLength; i++) {
            String sourceTopic = subscribeTopics[i];

            if (!isWildCard(sourceTopic)) {
                if (!sourceTopic.equals(messageTopics[i])) {
                    return false;
                }
            }
            // multi level
            // [MQTT-4.7.1-2] In either case '#' MUST be the last character specified in the Topic Filter
            // and "t/t1#" is invalid
            if (Constants.NUMBER_SIGN.equals(sourceTopic)) {
                return i == sourceTopicLength - 1;
            }
            boolean last = i == minTopicLength - 1 &&
                    (sourceTopicLength == targetTopicLength ||
                            sourceTopicLength == targetTopicLength + 1 &&
                                    Constants.NUMBER_SIGN.equals(subscribeTopics[sourceTopicLength - 1])

                    );
            if (last) {
                return true;
            }
        }

        return false;
    }

    public static String wrapLmq(String firstTopic, String secondTopic) {
        if (StringUtils.isBlank(secondTopic)) {
            return firstTopic;
        }
        return firstTopic + normalizeSecondTopic(secondTopic);
    }

    public static String wrapP2pLmq(String clientId) {
        return normalizeTopic(Constants.P2P + clientId);
    }
}
