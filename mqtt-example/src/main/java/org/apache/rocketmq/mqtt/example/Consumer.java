package org.apache.rocketmq.mqtt.example;

import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Consumer {

    public static void main(String[] args) throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        String brokerUrl = "tcp://127.0.0.1:1883";
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String firstTopic = "offline";
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
                System.out.println("in in in in");
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
        connOpts.setUserName("wlq");
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, "123").toCharArray());
        return connOpts;
    }

    private static String now() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sf.format(new Date()) + "\t";
    }
}
