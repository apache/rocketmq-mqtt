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

package org.apache.rocketmq.mqtt.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class RocketMQProducer {
    private static DefaultMQProducer producer;
    private static String firstTopic = System.getenv("firstTopic");
    private static String recvClientId = "recv01";

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        producer = new DefaultMQProducer("PID_TEST");
        // Specify name server addresses.
        producer.setNamesrvAddr(System.getenv("namesrv"));
        //Launch the instance.
        producer.start();

        for (int i = 0; i < 1000; i++) {
            //Create a message instance, specifying topic, tag and message body.

            //Call send message to deliver message to one of brokers.
            try {
                sendMessage(i);
                Thread.sleep(1000);
                sendWithWildcardMessage(i);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }

    private static void setLmq(Message msg, Set<String> queues) {
        msg.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH,
                StringUtils.join(
                        queues.stream().map(s -> StringUtils.replace(s, "/", "%")).map(s -> MixAll.LMQ_PREFIX + s).collect(Collectors.toSet()),
                        MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
    }

    private static void sendMessage(int i) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Message msg = new Message(firstTopic,
                "MQ2MQTT",
                ("MQ_" + System.currentTimeMillis() + "_" + i).getBytes(StandardCharsets.UTF_8));
        String secondTopic = "/r1";
        setLmq(msg, new HashSet<>(Arrays.asList(TopicUtils.wrapLmq(firstTopic, secondTopic))));
        SendResult sendResult = producer.send(msg);
        System.out.println(now() + "sendMessage: " + new String(msg.getBody()));
    }

    private static void sendWithWildcardMessage(int i) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Message msg = new Message(firstTopic,
                "MQ2MQTT",
                ("MQwc_" + System.currentTimeMillis() + "_" + i).getBytes(StandardCharsets.UTF_8));
        String secondTopic = "/r/wc";
        Set<String> lmqSet = new HashSet<>();
        lmqSet.add(TopicUtils.wrapLmq(firstTopic, secondTopic));
        lmqSet.addAll(mapWildCardLmq(firstTopic, secondTopic));
        setLmq(msg, lmqSet);
        SendResult sendResult = producer.send(msg);
        System.out.println(now() + "sendWcMessage: " + new String(msg.getBody()));
    }

    private static Set<String> mapWildCardLmq(String firstTopic, String secondTopic) {
        // todo by yourself
        return new HashSet<>(Arrays.asList(TopicUtils.wrapLmq(firstTopic, "/r/+")));
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }

}
