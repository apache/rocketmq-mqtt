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

package org.apache.rocketmq.mqtt.example;

import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MqttConsumer {
    public static void main(String[] args) throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        String brokerUrl = System.getenv("brokerUrl");
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String firstTopic = System.getenv("firstTopic");
        String recvClientId = "recv01";
        MqttConnectOptions mqttConnectOptions = buildMqttConnectOptions(recvClientId);
        MqttClient mqttClient = new MqttClient(brokerUrl, recvClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println(recvClientId + " connect success to " + serverURI);
                try {
                    final String topicFilter[] = {firstTopic + "/r1", firstTopic + "/r/+", firstTopic + "/r2"};
                    final int[] qos = {1, 1, 2};
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
                    String[] ss = payload.split("_");
                    System.out.println(now() + "receive:" + topic + "," + payload
                            + " ---- rt:" + (System.currentTimeMillis() - Long.parseLong(ss[1])));
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
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, System.getenv("secretKey")).toCharArray());
        return connOpts;
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }
}
