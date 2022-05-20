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

package org.apache.rocketmq.mqtt.ds.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;

import java.util.Properties;


public class MqPullConsumer {
    private DefaultMQPullConsumer defaultMQPullConsumer;

    public MqPullConsumer(Properties properties) {
        this(properties.getProperty("NAMESRV_ADDR"));
    }

    public MqPullConsumer(String nameSrv) {
        defaultMQPullConsumer = new DefaultMQPullConsumer();
        defaultMQPullConsumer.setNamesrvAddr(nameSrv);
        defaultMQPullConsumer.setInstanceName(this.buildInstanceName());
        defaultMQPullConsumer.setVipChannelEnabled(false);
    }

    public String buildInstanceName() {
        return Integer.toString(UtilAll.getPid())
                + "#" + System.nanoTime();
    }

    public void setConsumerGroup(String consumerGroup) {
        defaultMQPullConsumer.setConsumerGroup(consumerGroup);
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    public void start() {
        try {
            defaultMQPullConsumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                defaultMQPullConsumer.shutdown();
            }
        });
    }

}
