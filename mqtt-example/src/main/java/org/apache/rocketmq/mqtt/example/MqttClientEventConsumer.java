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

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mqtt.common.model.ClientEvent;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class MqttClientEventConsumer {

    private static void subscribeWithRocketMQConsumer() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GID_test01");

        // Specify name server addresses.
        consumer.setNamesrvAddr(System.getenv("namesrv"));

        // Subscribe one more more topics to consume.
        String firstTopic = Constants.MQTT_SYSTEM_TOPIC;
        consumer.subscribe(firstTopic, Constants.CLIENT_EVENT_TAG);
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                ClientEvent clientEvent = JSON.parseObject(new String(messageExt.getBody()), ClientEvent.class);
                System.out.println(now() + "Receive client event: " + clientEvent);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();
        System.out.printf("RocketMQ Consumer Started.%n");
    }

    private static void subscribeWithMQTTConsumer() throws NoSuchAlgorithmException, InvalidKeyException {
        String brokerUrl = "tcp://" + System.getenv("host") + ":1883";
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String recvClientId = "recv01";
        MqttConnectOptions mqttConnectOptions = buildMqttConnectOptions(recvClientId);
        MqttClient mqttClient = new MqttClient(brokerUrl, recvClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println(recvClientId + " connect success to " + serverURI);
                try {
                    mqttClient.subscribe(Constants.CLIENT_EVENT_ORIGIN_TOPIC, 1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void connectionLost(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                try {
                    String payload = new String(mqttMessage.getPayload());
                    ClientEvent clientEvent = JSON.parseObject(payload, ClientEvent.class);
                    System.out.println(now() + "receive client event:" + clientEvent);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        try {
            mqttClient.connect(mqttConnectOptions);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("connect fail");
        }
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }

    private static MqttConnectOptions buildMqttConnectOptions(String clientId)
            throws NoSuchAlgorithmException, InvalidKeyException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxInflight(10000);
        connOpts.setUserName(System.getenv("username"));
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, System.getenv("password")).toCharArray());
        return connOpts;
    }

    public static void main(String[] args)
            throws MQClientException, NoSuchAlgorithmException, InvalidKeyException {
        subscribeWithRocketMQConsumer();
        subscribeWithMQTTConsumer();
    }
}