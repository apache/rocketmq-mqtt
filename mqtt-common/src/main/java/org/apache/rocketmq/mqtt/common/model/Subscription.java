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

package org.apache.rocketmq.mqtt.common.model;

import org.apache.rocketmq.mqtt.common.util.TopicUtils;


public class Subscription {
    private String topicFilter;
    private int qos;

    public Subscription() {
    }

    public Subscription(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public Subscription(String topicFilter, int qos) {
        this.topicFilter = topicFilter;
        this.qos = qos;
    }

    public boolean isWildCard() {
        return topicFilter != null &&
                (topicFilter.contains(Constants.JINFLAG) || topicFilter.contains(Constants.ADDFLAG));
    }

    public String toFirstTopic() {
        return TopicUtils.decode(topicFilter).getFirstTopic();
    }

    public String toQueueName() {
        return topicFilter;
    }

    public static Subscription newP2pSubscription(String clientId) {
        Subscription p2pSubscription = new Subscription();
        p2pSubscription.setTopicFilter(TopicUtils.getP2pTopic(clientId));
        p2pSubscription.setQos(1);
        return p2pSubscription;
    }

    public static Subscription newRetrySubscription(String clientId) {
        Subscription retrySubscription = new Subscription();
        retrySubscription.setTopicFilter(TopicUtils.getRetryTopic(clientId));
        retrySubscription.setQos(1);
        return retrySubscription;
    }

    public boolean isRetry() {
        return TopicUtils.isRetryTopic(topicFilter);
    }

    public boolean isP2p() {
        return TopicUtils.isP2pTopic(topicFilter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        Subscription that = (Subscription)o;

        return topicFilter != null ? topicFilter.equals(that.topicFilter) : that.topicFilter == null;
    }

    @Override
    public int hashCode() {
        return topicFilter != null ? topicFilter.hashCode() : 0;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    @Override
    public String toString() {
        return "Subscription{" +
            "topicFilter='" + topicFilter + '\'' +
            ", qos=" + qos +
            '}';
    }
}
