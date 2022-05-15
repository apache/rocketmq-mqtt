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

package org.apache.rocketmq.mqtt.ds.test.mq;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mqtt.ds.mq.MqFactory;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class TestMqFactory {

    final String nameSrv = "test";
    final String NAMESRV_ADDR = "NAMESRV_ADDR";
    final String group = "group";

    private MessageListener messageListener;

    @Before
    public void SetUp() {
        messageListener = new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                return null;
            }
        };
    }

    @Test
    public void test() {
        Properties properties = new Properties();
        properties.setProperty(NAMESRV_ADDR, nameSrv);

        DefaultMQProducer mqProducer = MqFactory.buildDefaultMQProducer(group, properties);
        Assert.assertEquals(nameSrv, mqProducer.getNamesrvAddr());

        DefaultMQAdminExt mqAdminExt = MqFactory.buildDefaultMQAdminExt(group, properties);
        Assert.assertEquals(group, mqAdminExt.getAdminExtGroup());

        DefaultMQPushConsumer mqPushConsumer = MqFactory.buildDefaultMQPushConsumer(group, properties, messageListener);
        Assert.assertEquals(group, mqPushConsumer.getConsumerGroup());

        DefaultMQPullConsumer mqPullConsumer = MqFactory.buildDefaultMQPullConsumer(group, properties);
        Assert.assertEquals(nameSrv, mqPullConsumer.getNamesrvAddr());

        mqProducer = MqFactory.buildDefaultMQProducer(group, nameSrv);
        Assert.assertEquals(nameSrv, mqProducer.getNamesrvAddr());

        mqPushConsumer = MqFactory.buildDefaultMQPushConsumer(group, nameSrv, messageListener, properties);
        Assert.assertEquals(group, mqPushConsumer.getConsumerGroup());

        mqPullConsumer = MqFactory.buildDefaultMQPullConsumer(group, nameSrv);
        Assert.assertEquals(nameSrv, mqPullConsumer.getNamesrvAddr());

        mqAdminExt = MqFactory.buildDefaultMQAdminExt(group, nameSrv);
        Assert.assertEquals(group, mqAdminExt.getAdminExtGroup());
    }
}
