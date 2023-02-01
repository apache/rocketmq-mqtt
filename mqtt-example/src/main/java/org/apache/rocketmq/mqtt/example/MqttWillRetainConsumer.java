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

import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MqttWillRetainConsumer {
    public static void main(String[] args) throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        String brokerUrl = "tcp://" + System.getenv("host") + ":1883";
        String firstTopic = System.getenv("topic");
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String recvClientId = "recv02";
        MqttConnectOptions mqttConnectOptions = buildMqttConnectOptions(recvClientId);
        MqttClient mqttClient = new MqttClient(brokerUrl, recvClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println(recvClientId + " connect success to " + serverURI);
                try {
                    final String topicFilter[] = {firstTopic + "/r3", firstTopic + "/retainTopic/+", firstTopic + "/willTopic1",};
                    final int[] qos = {1, 1, 1};
                    mqttClient.subscribe(topicFilter, qos);
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
                    System.out.println(now() + "receive:" + topic + "," + payload);
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

    private static MqttConnectOptions buildMqttConnectOptions(String clientId) throws NoSuchAlgorithmException, InvalidKeyException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxInflight(10000);
        connOpts.setUserName(System.getenv("username"));
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, System.getenv("password")).toCharArray());
        return connOpts;
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }

}