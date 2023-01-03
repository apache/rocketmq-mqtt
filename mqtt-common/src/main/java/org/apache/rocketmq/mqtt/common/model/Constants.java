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


    public static final String PROPERTY_MQTT_MSG_EVENT_RETRY_NODE = "retryNode";
    public static final String PROPERTY_MQTT_MSG_EVENT_RETRY_TIME = "retryTime";

    public static final String MQTT_TAG = "MQTT_COMMON";

    public static final String PROPERTY_ORIGIN_MQTT_ISEMPTY_MSG = "IS_EMPTY_MSG";

    public static final String CS_ALIVE = "alive";

    public static final String CS_MASTER = "master";

    public static final byte CTRL_0 = '\u0000';

    public static final byte CTRL_1 = '\u0001';

    public static final byte CTRL_2 = '\u0002';

    public static final String NOT_FOUND = "NOT_FOUNT";

}
