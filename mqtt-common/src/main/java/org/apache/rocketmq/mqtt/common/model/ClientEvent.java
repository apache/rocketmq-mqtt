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

import com.alibaba.fastjson.JSON;

public class ClientEvent {
    private String clientId;
    private String channelId;
    private EventType eventType;
    private long eventTime;
    private String host;
    private String ip;
    private int port;
    private int packetId;
    private String eventInfo;

    public ClientEvent(EventType eventType) {
        this.eventType = eventType;
        this.eventTime = System.currentTimeMillis();
    }

    public String getClientId() {
        return clientId;
    }

    public ClientEvent setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public String getChannelId() {
        return channelId;
    }

    public ClientEvent setChannelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    public int getPacketId() {
        return packetId;
    }

    public ClientEvent setPacketId(int packetId) {
        this.packetId = packetId;
        return this;
    }

    public EventType getEventType() {
        return eventType;
    }

    public ClientEvent setEventType(EventType eventType) {
        this.eventType = eventType;
        return this;
    }

    public long getEventTime() {
        return eventTime;
    }

    public ClientEvent setEventTime(long eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public String getHost() {
        return host;
    }

    public ClientEvent setHost(String host) {
        this.host = host;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public ClientEvent setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ClientEvent setPort(int port) {
        this.port = port;
        return this;
    }

    public String getEventInfo() {
        return eventInfo;
    }

    public ClientEvent setEventInfo(String eventInfo) {
        this.eventInfo = eventInfo;
        return this;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
