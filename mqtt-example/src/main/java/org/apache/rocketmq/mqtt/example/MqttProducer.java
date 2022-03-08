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

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MqttProducer {
    public static void main(String[] args) throws InterruptedException, MqttException, NoSuchAlgorithmException, InvalidKeyException {
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String brokerUrl = System.getenv("brokerUrl");
        String firstTopic = System.getenv("firstTopic");
        String sendClientId = "send01";
        String recvClientId = "recv01";
        MqttConnectOptions mqttConnectOptions = buildMqttConnectOptions(sendClientId);
        MqttClient mqttClient = new MqttClient(brokerUrl, sendClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println(sendClientId + " connect success to " + serverURI);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });
        try {
            mqttClient.connect(mqttConnectOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long interval = 1000;
        for (int i = 0; i < 1000; i++) {
            String msg = "r1_" + System.currentTimeMillis() + "_" + i;
            MqttMessage message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
            message.setQos(1);
            String mqttSendTopic = firstTopic + "/r1";
            mqttClient.publish(mqttSendTopic, message);
            System.out.println(now() + "send: " + mqttSendTopic + ", " + msg);
            Thread.sleep(interval);

            mqttSendTopic = firstTopic + "/r/wc";
            msg = "wc_" + System.currentTimeMillis() + "_" + i;
            MqttMessage messageWild = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
            messageWild.setQos(1);
            mqttClient.publish(mqttSendTopic, messageWild);
            System.out.println(now() + "send: " + mqttSendTopic + ", " + msg);
            Thread.sleep(interval);

            mqttSendTopic = firstTopic + "/r2";
            msg = "msgQ2_" + System.currentTimeMillis() + "_" + i;
            message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
            message.setQos(2);
            mqttClient.publish(mqttSendTopic, message);
            System.out.println(now() + "send: " + mqttSendTopic + ", " + msg);
            Thread.sleep(interval);
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
