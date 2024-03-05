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

package org.apache.rocketmq.mqtt.cs.channel;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REQUEST_RESPONSE_INFORMATION;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM;

public class ChannelInfo {
    private static final String CHANNEL_ID_KEY = "0";
    private static final String CHANNEL_CLIENT_ID_KEY = "1";
    private static final String CHANNEL_TIMESTAMP_KEY = "2";
    private static final String CHANNEL_KEEPLIVE_KEY = "3";
    private static final String CHANNEL_OWNER_KEY = "4";
    private static final String CHANNEL_NAMESPACE_KEY = "5";
    private static final String CHANNEL_EXT_CHANGE_KEY = "6";
    private static final String CHANNEL_STORE_NAMESPACE_KEY = "7";
    private static final String CHANNEL_CLEAN_SESSION_KEY = "8";
    private static final String CHANNEL_SUB_NUM_KEY = "9";
    private static final String CHANNEL_FUTRUE_KEY = "10";
    private static final String CHANNEL_LIFE_CYCLE = "11";
    private static final String CHANNEL_LAST_ACTIVE_TIMESTAMP_KEY = "12";
    private static final String CHANNEL_IS_FLUSHING = "13";
    private static final String CHANNEL_REMOTE_IP = "14";
    private static final String CHANNEL_TUNNEL_ID = "15";

    public static final String FUTURE_CONNECT = "connect";
    public static final String FUTURE_SUBSCRIBE = "subscribe";

    public static final AttributeKey<Map<String, Object>> CHANNEL_INFO_ATTRIBUTE_KEY = AttributeKey.valueOf("I");

    public static final AttributeKey<ConcurrentMap<String, String>> CHANNEL_EXTDATA_ATTRIBUTE_KEY = AttributeKey
        .valueOf("E");
    private static final AttributeKey<MqttVersion> CHANNEL_VERSION_ATTRIBUTE_KEY = AttributeKey.valueOf("V");

    public static final AttributeKey<String> CHANNEL_GA_ATTRIBUTE_KEY = AttributeKey.valueOf("GA");

    public static final AttributeKey<Map<MqttProperties.MqttPropertyType, Object>> CHANNEL_INFO_MQTT5_ATTRIBUTE_KEY = AttributeKey.valueOf("I5");

    public static final AttributeKey<List<MqttProperties.UserProperty>> CHANNEL_USER_PROPERTY_MQTT5_ATTRIBUTE_KEY = AttributeKey.valueOf("UP");

    public static Map<String, String> getExtData(Channel channel) {
        Attribute<ConcurrentMap<String, String>> extAttribute = channel.attr(CHANNEL_EXTDATA_ATTRIBUTE_KEY);
        if (extAttribute.get() == null) {
            extAttribute.setIfAbsent(new ConcurrentHashMap<>());
        }
        return extAttribute.get();
    }

    public static String encodeExtData(Channel channel) {
        Map<String, String> extData = getExtData(channel);
        return JSON.toJSONString(extData);
    }

    public static boolean updateExtData(Channel channel, String extDataStr) {
        if (StringUtils.isBlank(extDataStr)) {
            return false;
        }
        updateExtData(channel, JSON.parseObject(extDataStr, Map.class));
        return true;
    }

    private static void updateExtData(Channel channel, Map<String, String> extData) {
        Map<String, String> currentExt = getExtData(channel);
        currentExt.putAll(extData);
        setExtDataChange(channel, true);
    }

    public static void setExtDataChange(Channel channel, boolean flag) {
        getInfo(channel).put(CHANNEL_EXT_CHANGE_KEY, flag);
    }

    public static boolean checkExtDataChange(Channel channel) {
        if (!getInfo(channel).containsKey(CHANNEL_EXT_CHANGE_KEY)) {
            getInfo(channel).put(CHANNEL_EXT_CHANGE_KEY, false);
        }
        Object obj = getInfo(channel).get(CHANNEL_EXT_CHANGE_KEY);
        if (obj == null) {
            return false;
        }
        return (boolean)obj;
    }

    public static String getId(Channel channel) {
        Map<String, Object> info = getInfo(channel);
        if (info.containsKey(CHANNEL_ID_KEY)) {
            return (String)info.get(CHANNEL_ID_KEY);
        }
        String channelIdStr = UUID.randomUUID().toString().replaceAll("-", "").toLowerCase();
        info.put(CHANNEL_ID_KEY, channelIdStr);
        return channelIdStr;
    }

    public static Boolean getCleanSessionFlag(Channel channel) {
        if (!getInfo(channel).containsKey(CHANNEL_CLEAN_SESSION_KEY)) {
            getInfo(channel).put(CHANNEL_CLEAN_SESSION_KEY, true);
        }
        Object obj = getInfo(channel).get(CHANNEL_CLEAN_SESSION_KEY);
        if (obj == null) {
            return true;
        }
        return (Boolean)obj;
    }

    public static void setCleanSessionFlag(Channel channel, Boolean cleanSessionFalg) {
        getInfo(channel).put(CHANNEL_CLEAN_SESSION_KEY, cleanSessionFalg);
    }

    public static String getClientId(Channel channel) {
        return (String)getInfo(channel).get(CHANNEL_CLIENT_ID_KEY);
    }

    public static long getChannelLifeCycle(Channel channel) {
        Long expireTime = (Long) getInfo(channel).get(CHANNEL_LIFE_CYCLE);
        if (expireTime == null) {
            return Long.MAX_VALUE;
        }
        return expireTime;
    }

    public static void setChannelLifeCycle(Channel channel, Long expireTime) {
        getInfo(channel).put(CHANNEL_LIFE_CYCLE, expireTime == null ? Long.MAX_VALUE : expireTime);
    }

    public static void setFuture(Channel channel, String futureKey, CompletableFuture<Void> future) {
        getInfo(channel).put(CHANNEL_FUTRUE_KEY + futureKey, future);
    }

    public static CompletableFuture<Void> getFuture(Channel channel, String futureKey) {
        Object future = getInfo(channel).get(CHANNEL_FUTRUE_KEY + futureKey);
        if (future != null) {
            return (CompletableFuture<Void>)future;
        }
        return null;
    }

    public static void removeFuture(Channel channel, String futureKey) {
        getInfo(channel).remove(CHANNEL_FUTRUE_KEY + futureKey);
    }

    public static void setClientId(Channel channel, String clientId) {
        getInfo(channel).put(CHANNEL_CLIENT_ID_KEY, clientId);
    }

    public static void touch(Channel channel) {
        getInfo(channel).put(CHANNEL_TIMESTAMP_KEY, System.currentTimeMillis());
    }

    public static long getLastTouch(Channel channel) {
        Object t = getInfo(channel).get(CHANNEL_TIMESTAMP_KEY);
        return t != null ? (long)t : 0;
    }

    public static void lastActive(Channel channel, long timeStamp) {
        getInfo(channel).put(CHANNEL_LAST_ACTIVE_TIMESTAMP_KEY, timeStamp);
    }

    public static long getLastActive(Channel channel) {
        Object t = getInfo(channel).get(CHANNEL_LAST_ACTIVE_TIMESTAMP_KEY);
        return t != null ? (long)t : 0;
    }

    public static void setRemoteIP(Channel channel, String ip) {
        getInfo(channel).put(CHANNEL_REMOTE_IP, ip);
    }

    public static String getRemoteIP(Channel channel) {
        Object t = getInfo(channel).get(CHANNEL_REMOTE_IP);
        return t == null ? "" : (String) t;
    }

    public static void setKeepLive(Channel channel, int seconds) {
        getInfo(channel).put(CHANNEL_KEEPLIVE_KEY, seconds);
    }

    public static Integer getKeepLive(Channel channel) {
        return (Integer)getInfo(channel).get(CHANNEL_KEEPLIVE_KEY);
    }

    public static boolean isExpired(Channel channel) {
        Long timestamp = (Long)getInfo(channel).get(CHANNEL_TIMESTAMP_KEY);
        if (timestamp == null) {
            return true;
        }
        Integer keepLiveT = getKeepLive(channel);
        if (keepLiveT == null) {
            return true;
        }
        return System.currentTimeMillis() - timestamp > keepLiveT * 1000L * 1.5;
    }

    public static void setOwner(Channel channel, String owner) {
        getInfo(channel).put(CHANNEL_OWNER_KEY, owner);
    }

    public static String getOwner(Channel channel) {
        return (String)getInfo(channel).get(CHANNEL_OWNER_KEY);
    }

    public static void setNamespace(Channel channel, String namespace) {
        getInfo(channel).put(CHANNEL_NAMESPACE_KEY, namespace);
    }

    public static String getNamespace(Channel channel) {
        return (String)getInfo(channel).get(CHANNEL_NAMESPACE_KEY);
    }

    /**
     * clear channelInfo except the channelId namespace
     *
     * @param channel
     */
    public static void clear(Channel channel) {
        String channelId = getId(channel);
        String namespace = getNamespace(channel);
        Map<String, Object> newInfoAttribute = new ConcurrentHashMap<>(8);
        newInfoAttribute.put(CHANNEL_ID_KEY, channelId);
        if (namespace != null) {
            newInfoAttribute.put(CHANNEL_NAMESPACE_KEY, namespace);
        }
        channel.attr(CHANNEL_INFO_ATTRIBUTE_KEY).set(newInfoAttribute);
        channel.attr(CHANNEL_EXTDATA_ATTRIBUTE_KEY).set(null);
        channel.attr(CHANNEL_INFO_MQTT5_ATTRIBUTE_KEY).set(null);
    }

    public static Map<String, Object> getInfo(Channel channel) {
        Attribute<Map<String, Object>> infoAttribute = channel.attr(CHANNEL_INFO_ATTRIBUTE_KEY);
        if (infoAttribute.get() == null) {
            infoAttribute.setIfAbsent(new ConcurrentHashMap<>(8));
        }
        return infoAttribute.get();
    }

    public static List<MqttProperties.UserProperty> getUserProperties(Channel channel) {
        return channel.attr(CHANNEL_USER_PROPERTY_MQTT5_ATTRIBUTE_KEY).get();
    }

    public static void setUserProperty(Channel channel, List<MqttProperties.UserProperty> userProperties) {
        channel.attr(CHANNEL_USER_PROPERTY_MQTT5_ATTRIBUTE_KEY).set(userProperties);
    }

    public static MqttVersion getMqttVersion(Channel channel) {
        return channel.attr(CHANNEL_VERSION_ATTRIBUTE_KEY).get();
    }

    public static void setMqttVersion(Channel channel, MqttVersion mqttVersion) {
        channel.attr(CHANNEL_VERSION_ATTRIBUTE_KEY).setIfAbsent(mqttVersion);
    }


    public static Map<MqttProperties.MqttPropertyType, Object> getMqtt5Info(Channel channel) {
        Attribute<Map<MqttProperties.MqttPropertyType, Object>> infoMqtt5Attribute = channel.attr(CHANNEL_INFO_MQTT5_ATTRIBUTE_KEY);
        if (infoMqtt5Attribute.get() == null) {
            infoMqtt5Attribute.setIfAbsent(new ConcurrentHashMap<>(8));
        }
        return infoMqtt5Attribute.get();
    }

    public static Integer getSessionExpiryInterval(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(SESSION_EXPIRY_INTERVAL);
    }

    public static void setSessionExpiryInterval(Channel channel, Integer sessionExpiryInterval) {
        getMqtt5Info(channel).put(SESSION_EXPIRY_INTERVAL, sessionExpiryInterval);
    }

    public static Integer getReceiveMaximum(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(RECEIVE_MAXIMUM);
    }

    public static void setReceiveMaximum(Channel channel, Integer receiveMaximum) {
        getMqtt5Info(channel).put(RECEIVE_MAXIMUM, receiveMaximum);
    }

    public static Integer getMaximumPacketSize(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(MAXIMUM_PACKET_SIZE);
    }

    public static void setMaximumPacketSize(Channel channel, Integer maximumPacketSize) {
        getMqtt5Info(channel).put(MAXIMUM_PACKET_SIZE, maximumPacketSize);
    }

    public static Integer getTopicAliasMaximum(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(TOPIC_ALIAS_MAXIMUM);
    }

    public static void setTopicAliasMaximum(Channel channel, Integer topicAliasMaximum) {
        getMqtt5Info(channel).put(TOPIC_ALIAS_MAXIMUM, topicAliasMaximum);
    }

    public static Boolean getRequestResponseInformation(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(REQUEST_PROBLEM_INFORMATION) == 1;
    }

    public static void setRequestResponseInformation(Channel channel, Integer requestResponseInformation) {
        getMqtt5Info(channel).put(REQUEST_PROBLEM_INFORMATION, requestResponseInformation);
    }

    public static Boolean getRequestProblemInformation(Channel channel) {
        return (Integer) getMqtt5Info(channel).get(REQUEST_RESPONSE_INFORMATION) == 1;
    }

    public static void setRequestProblemInformation(Channel channel, Integer requestProblemInformation) {
        getMqtt5Info(channel).put(REQUEST_RESPONSE_INFORMATION, requestProblemInformation);
    }
}
