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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;


public class Queue {
    protected long queueId;
    protected String queueName;
    protected String brokerName;

    public Queue() {
    }

    public Queue(long queueId, String queueName, String brokerName) {
        this.queueId = queueId;
        this.queueName = queueName;
        this.brokerName = brokerName;
    }

    public boolean isLmq() {
        return StringUtils.isNotBlank(brokerName);
    }

    public String toFirstTopic() {
        return TopicUtils.decode(queueName).getFirstTopic();
    }

    public boolean isRetry() {
        return TopicUtils.isRetryTopic(queueName);
    }

    public boolean isP2p() {
        return TopicUtils.isP2pTopic(queueName);
    }

    public long getQueueId() {
        return queueId;
    }

    public void setQueueId(long queueId) {
        this.queueId = queueId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Queue queue = (Queue) o;

        if (queueId != queue.queueId) {
            return false;
        }
        if (queueName != null ? !queueName.equals(queue.queueName) : queue.queueName != null) {
            return false;
        }
        return brokerName != null ? brokerName.equals(queue.brokerName) : queue.brokerName == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (queueId ^ (queueId >>> 32));
        result = 31 * result + (queueName != null ? queueName.hashCode() : 0);
        result = 31 * result + (brokerName != null ? brokerName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Queue{" +
                "queueId=" + queueId +
                ", queueName='" + queueName + '\'' +
                ", brokerName='" + brokerName + '\'' +
                '}';
    }
}
