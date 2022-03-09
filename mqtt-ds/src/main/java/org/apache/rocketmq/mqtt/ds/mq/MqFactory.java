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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Properties;


public class MqFactory {
    public static synchronized DefaultMQProducer buildDefaultMQProducer(String group, Properties properties) {
        MqProducer mqProducer = new MqProducer(properties);
        mqProducer.setProducerGroup(group);
        return mqProducer.getDefaultMQProducer();
    }

    public static synchronized DefaultMQAdminExt buildDefaultMQAdminExt(String group, Properties properties) {
        MqAdmin mqadmin = new MqAdmin(properties);
        mqadmin.setAdminGroup(group);
        return mqadmin.getDefaultMQAdminExt();
    }

    public static synchronized DefaultMQPushConsumer buildDefaultMQPushConsumer(String group, Properties properties,
                                                                                MessageListener messageListener) {
        MqConsumer mqConsumer = new MqConsumer(properties);
        mqConsumer.setConsumerGroup(group);
        mqConsumer.setMessageListener(messageListener);
        return mqConsumer.getDefaultMQPushConsumer();
    }

    public static synchronized DefaultMQPullConsumer buildDefaultMQPullConsumer(String group, Properties properties) {
        MqPullConsumer mqConsumer = new MqPullConsumer(properties);
        mqConsumer.setConsumerGroup(group);
        return mqConsumer.getDefaultMQPullConsumer();
    }

    public static DefaultMQProducer buildDefaultMQProducer(String group, String nameSrv) {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
        defaultMQProducer.setNamesrvAddr(nameSrv);
        defaultMQProducer.setInstanceName(buildIntanceName());
        defaultMQProducer.setVipChannelEnabled(false);
        defaultMQProducer.setProducerGroup(group);
        return defaultMQProducer;
    }

    public static DefaultMQPushConsumer buildDefaultMQPushConsumer(String group, String nameSrv,
                                                                   MessageListener messageListener, Properties properties) {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer();
        defaultMQPushConsumer.setNamesrvAddr(nameSrv);
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(1);
        defaultMQPushConsumer.setPullBatchSize(Integer.parseInt(properties.getProperty("pullBatch", "64")));
        if (properties.get(MqConsumer.THREAD_NUM_KEY) != null) {
            defaultMQPushConsumer.setConsumeThreadMin(Integer.valueOf((String) properties.get("threadNum")));
            defaultMQPushConsumer.setConsumeThreadMax(Integer.valueOf((String) properties.get("threadNum")));
        }
        defaultMQPushConsumer.setInstanceName(buildIntanceName());
        defaultMQPushConsumer.setVipChannelEnabled(false);
        defaultMQPushConsumer.setConsumerGroup(group);
        if (messageListener instanceof MessageListenerOrderly) {
            defaultMQPushConsumer.registerMessageListener((MessageListenerOrderly) messageListener);
        } else {
            defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) messageListener);
        }
        return defaultMQPushConsumer;
    }

    public static DefaultMQPullConsumer buildDefaultMQPullConsumer(String group, String nameSrv) {
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer();
        defaultMQPullConsumer.setNamesrvAddr(nameSrv);
        defaultMQPullConsumer.setInstanceName(buildIntanceName());
        defaultMQPullConsumer.setVipChannelEnabled(false);
        defaultMQPullConsumer.setConsumerGroup(group);
        return defaultMQPullConsumer;
    }

    public static DefaultMQAdminExt buildDefaultMQAdminExt(String group, String nameSrv) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr(nameSrv);
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setAdminExtGroup(group);
        return defaultMQAdminExt;
    }

    public static String buildIntanceName() {
        return Integer.toString(UtilAll.getPid())
                + "#" + System.nanoTime();
    }

}
