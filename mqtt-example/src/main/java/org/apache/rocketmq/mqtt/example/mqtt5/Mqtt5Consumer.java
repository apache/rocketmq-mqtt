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

package org.apache.rocketmq.mqtt.example.mqtt5;

import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;


import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Mqtt5Consumer {
    public static void main(String[] args) throws MqttException, NoSuchAlgorithmException, InvalidKeyException, org.eclipse.paho.mqttv5.common.MqttException {
        String brokerUrl = "tcp://" + System.getenv("host") + ":1883";
        String firstTopic = System.getenv("topic");
        org.eclipse.paho.mqttv5.client.persist.MemoryPersistence memoryPersistence = new MemoryPersistence();
        String recvClientId = "recv01";

        MqttConnectionOptions mqttConnectionOptions = buildMqttConnectionOptions(recvClientId);
        MqttClient mqttClient = new MqttClient(brokerUrl, recvClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void disconnected(MqttDisconnectResponse disconnectResponse) {
                System.out.println(recvClientId + " disconnect success to " + disconnectResponse.getReasonString());
            }

            @Override
            public void mqttErrorOccurred(MqttException exception) {
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                try {
                    String payload = new String(message.getPayload());
                    String[] ss = payload.split("_");
                    System.out.println(now() + "receive:" + topic + "," + payload);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void deliveryComplete(IMqttToken token) {
            }

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
            public void authPacketArrived(int reasonCode, MqttProperties properties) {
            }

        });

        try {
            mqttClient.connect(mqttConnectionOptions);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("connect fail");
        }
    }

    private static MqttConnectionOptions buildMqttConnectionOptions(String clientId) throws NoSuchAlgorithmException, InvalidKeyException {
        MqttConnectionOptions connOpts = new MqttConnectionOptions();
        connOpts.setCleanStart(true);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setUserName(System.getenv("username"));
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, System.getenv("password")).getBytes(StandardCharsets.UTF_8));
        connOpts.setTopicAliasMaximum(10);
        return connOpts;
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }
}