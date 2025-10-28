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
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    public static final AttributeKey<String> CHANNEL_GA_ATTRIBUTE_KEY = AttributeKey.valueOf("GA");

    public static final AttributeKey<Boolean> CHANNEL_CONNECTED_ATTRIBUTE_KEY = AttributeKey.valueOf("C");


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
    }

    public static Map<String, Object> getInfo(Channel channel) {
        Attribute<Map<String, Object>> infoAttribute = channel.attr(CHANNEL_INFO_ATTRIBUTE_KEY);
        if (infoAttribute.get() == null) {
            infoAttribute.setIfAbsent(new ConcurrentHashMap<>(8));
        }
        return infoAttribute.get();
    }

}
