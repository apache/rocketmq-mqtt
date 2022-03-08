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

package org.apache.rocketmq.mqtt.ds.mq;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;

import java.util.Properties;

public class MqConsumer  {
    public static final String THREAD_NUM_KEY = "threadNum";
    private DefaultMQPushConsumer defaultMQPushConsumer;

    public MqConsumer(Properties properties) {
        defaultMQPushConsumer = new DefaultMQPushConsumer();
        defaultMQPushConsumer.setNamesrvAddr(properties.getProperty("NAMESRV_ADDR"));
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(1);
        defaultMQPushConsumer.setPullBatchSize(Integer.parseInt(properties.getProperty("pullBatch", "64")));
        if (properties.get(THREAD_NUM_KEY) != null) {
            defaultMQPushConsumer.setConsumeThreadMin(Integer.valueOf((String)properties.get("threadNum")));
            defaultMQPushConsumer.setConsumeThreadMax(Integer.valueOf((String)properties.get("threadNum")));
        }
        defaultMQPushConsumer.setInstanceName(this.buildIntanceName());
        defaultMQPushConsumer.setVipChannelEnabled(false);
    }

    public String buildIntanceName() {
        return Integer.toString(UtilAll.getPid())
                + "#" + System.nanoTime();
    }

    public void setConsumerGroup(String consumerGroup) {
        defaultMQPushConsumer.setConsumerGroup(consumerGroup);
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public void setMessageListener(MessageListener messageListener) {
        if (messageListener instanceof MessageListenerOrderly) {
            defaultMQPushConsumer.registerMessageListener((MessageListenerOrderly)messageListener);
        } else {
            defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently)messageListener);
        }
    }

    public void start() {
        try {
            defaultMQPushConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                defaultMQPushConsumer.shutdown();
            }
        });
    }

}
