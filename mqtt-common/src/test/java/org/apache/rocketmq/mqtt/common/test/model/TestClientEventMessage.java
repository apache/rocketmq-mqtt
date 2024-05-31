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

package org.apache.rocketmq.mqtt.common.test.model;

import org.apache.rocketmq.mqtt.common.model.ClientEventMessage;
import org.apache.rocketmq.mqtt.common.model.ClientEventType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestClientEventMessage {
  final String clientId = "testClientId";
  final String channelId = "testChannelId";
  final int packetId = 123;
  final String host = "testHost";
  final String ip = "testIp";
  final int port = 8080;

  @Test
  public void test() {
    ClientEventMessage clientEventMessage = new ClientEventMessage(ClientEventType.CONNECT);
    clientEventMessage.setClientId(clientId);
    clientEventMessage.setChannelId(channelId);
    clientEventMessage.setPacketId(packetId);
    clientEventMessage.setHost(host);
    clientEventMessage.setIp(ip);
    clientEventMessage.setPort(port);

    assertEquals(clientId, clientEventMessage.getClientId());
    assertEquals(channelId, clientEventMessage.getChannelId());
    assertEquals(packetId, clientEventMessage.getPacketId());
    assertEquals(ClientEventType.CONNECT, clientEventMessage.getEventType());
    assertEquals(host, clientEventMessage.getHost());
    assertEquals(ip, clientEventMessage.getIp());
    assertEquals(port, clientEventMessage.getPort());

    String expectedJson = "{\"channelId\":\"testChannelId\",\"clientId\":\"testClientId\",\"eventTime\":" +
        clientEventMessage.getEventTime() +
        ",\"eventType\":\"CONNECT\",\"host\":\"testHost\",\"ip\":\"testIp\",\"packetId\":123,\"port\":8080}";
    assertEquals(expectedJson, clientEventMessage.toString());
  }
}
