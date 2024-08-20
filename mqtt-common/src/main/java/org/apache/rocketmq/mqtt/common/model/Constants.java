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

package org.apache.rocketmq.mqtt.common.model;

public class Constants {
    public static final String NAMESPACE_SPLITER = "%";
    public static final String MQTT_TOPIC_DELIMITER = "/";

    public static final String PLUS_SIGN = "+";
    public static final String NUMBER_SIGN = "#";
    public static final String DOLLAR_SIGN = "$";
    public static final String COLON = ":";

    public static final String P2P = "/p2p/";
    public static final String RETRY = "/retry/";

    public static final String PROPERTY_NAMESPACE = "namespace";
    public static final String PROPERTY_ORIGIN_MQTT_TOPIC = "originMqttTopic";
    public static final String PROPERTY_MQTT_QOS = "qosLevel";
    public static final String PROPERTY_MQTT_CLEAN_SESSION = "cleanSessionFlag";
    public static final String PROPERTY_MQTT_CLIENT = "clientId";
    public static final String PROPERTY_MQTT_RETRY_TIMES = "retryTimes";
    public static final String PROPERTY_MQTT_EXT_DATA = "extData";
    public static final String PROPERTY_CORRELATION_DATA = "correlationData";
    public static final String PROPERTY_PAYLOAD_FORMAT_INDICATOR = "payloadFormatIndicator";
    public static final String PROPERTY_MESSAGE_EXPIRY_INTERVAL = "messageExpiryInterval";
    public static final String PROPERTY_TOPIC_ALIAS = "topicAlias";
    public static final String PROPERTY_RESPONSE_TOPIC = "responseTopic";
    public static final String PROPERTY_MQTT5_USER_PROPERTY = "userProperty";
    public static final String PROPERTY_SUBSCRIPTION_IDENTIFIER = "subscriptionIdentifier";
    public static final String PROPERTY_CONTENT_TYPE = "contentType";


    public static final String PROPERTY_MQTT_MSG_EVENT_RETRY_NODE = "retryNode";
    public static final String PROPERTY_MQTT_MSG_EVENT_RETRY_TIME = "retryTime";

    public static final String MQTT_TAG = "MQTT_COMMON";

    public static final String PROPERTY_ORIGIN_MQTT_ISEMPTY_MSG = "IS_EMPTY_MSG";

    public static final String CS_ALIVE = "alive";

    public static final String CS_MASTER = "master";

    public static final byte CTRL_0 = '\u0000';

    public static final byte CTRL_1 = '\u0001';

    public static final byte CTRL_2 = '\u0002';

    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String SHARED_PREFIX = DOLLAR_SIGN + "share";
    public static final String EMPTY_SHARE_NAME = "";

    public static final String CLIENT_EVENT_TAG = "CLIENT_EVENT";
    public static final String MQTT_SYSTEM_TOPIC = "%SYS_MQTT";
    public static final String CLIENT_EVENT_SECOND_TOPIC = "online_offline_event";
    public static final String CLIENT_EVENT_ORIGIN_TOPIC = MQTT_SYSTEM_TOPIC
            + MQTT_TOPIC_DELIMITER + CLIENT_EVENT_SECOND_TOPIC;
    public static final int CLIENT_EVENT_BATCH_SIZE = 100;

    public static final int COAP_VERSION = 1;
    public static final int COAP_PAYLOAD_MARKER = 0xFF;
    public static final int COAP_MAX_TOKEN_LENGTH = 8;
    public static final String COAP_QUERY_DELIMITER = "=";
    public static final String COAP_PS_PREFIX = "ps";
    public static final String COAP_CONNECTION_PREFIX_1 = "mqtt";
    public static final String COAP_CONNECTION_PREFIX_2 = "connection";
    public static final String COAP_QUERY_CLIENT_ID = "clientid";
    public static final String COAP_QUERY_QOS = "qos";
    public static final String COAP_QUERY_RETAIN = "retain";
    public static final String COAP_QUERY_EXPIRY = "expiry";
    public static final String COAP_QUERY_USER_NAME = "username";
    public static final String COAP_QUERY_PASSWORD = "password";
    public static final String COAP_AUTH_TOKEN = "token";
}
