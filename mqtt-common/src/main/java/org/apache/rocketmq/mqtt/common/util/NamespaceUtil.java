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

package org.apache.rocketmq.mqtt.common.util;

import org.apache.commons.lang3.StringUtils;

public class NamespaceUtil {
    public static final String NAMESPACE_SPLITER = "%";
    private static int RESOURCE_LENGTH = 2;
    public static final String MQ_DEFAULT_NAMESPACE_NAME = "DEFAULT_INSTANCE";

    public NamespaceUtil() {
    }

    public static String encodeToNamespaceResource(String namespace, String resource) {
        return resource != null && namespace != null ? StringUtils.join(new String[]{namespace, "%", resource}) : resource;
    }

    public static String decodeOriginResource(String resource) {
        if (resource != null && resource.contains("%")) {
            int firstIndex = resource.indexOf("%");
            return resource.substring(firstIndex + 1);
        } else {
            return resource;
        }
    }

    public static String decodeMqttNamespaceIdFromKey(String key) {
        return decodeMqttNamespaceIdFromClientId(key);
    }

    public static String decodeMqttNamespaceIdFromClientId(String clientId) {
        if (clientId != null && clientId.contains("%")) {
            String mqttNamespaceId = clientId.split("%")[0];
            return mqttNamespaceId;
        } else {
            return null;
        }
    }

    public static String decodeStoreNamespaceIdFromTopic(String topic) {
        if (topic != null && topic.contains("%")) {
            String storeNamespaceId = topic.split("%")[0];
            return storeNamespaceId;
        } else {
            return null;
        }
    }

    public static String decodeNamespaceId(String resource) {
        return resource != null && resource.contains("%") ? resource.split("%")[0] : null;
    }
}
