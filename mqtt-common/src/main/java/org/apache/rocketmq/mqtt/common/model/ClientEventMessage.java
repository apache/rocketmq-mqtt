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

public class ClientEventMessage {
  private String clientId;
  private String channelId;
  private ClientEventType eventType;
  private long eventTime;
  private String host;
  private String ip;
  private int port;

  public ClientEventMessage(ClientEventType eventType) {
    this.eventType = eventType;
    this.eventTime = System.currentTimeMillis();
  }

  public String getClientId() {
    return clientId;
  }

  public ClientEventMessage setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public String getChannelId() {
    return channelId;
  }

  public ClientEventMessage setChannelId(String channelId) {
    this.channelId = channelId;
    return this;
  }

  public ClientEventType getEventType() {
    return eventType;
  }

  public ClientEventMessage setEventType(ClientEventType eventType) {
    this.eventType = eventType;
    return this;
  }

  public long getEventTime() {
    return eventTime;
  }

  public ClientEventMessage setEventTime(long eventTime) {
    this.eventTime = eventTime;
    return this;
  }

  public String getHost() {
    return host;
  }

  public ClientEventMessage setHost(String host) {
    this.host = host;
    return this;
  }

  public String getIp() {
    return ip;
  }

  public ClientEventMessage setIp(String ip) {
    this.ip = ip;
    return this;
  }

  public int getPort() {
    return port;
  }

  public ClientEventMessage setPort(int port) {
    this.port = port;
    return this;
  }
}
