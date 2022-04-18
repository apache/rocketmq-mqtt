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

package org.apache.rocketmq.mqtt.cs.test.protocol.ws;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketEncoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class TestWebSocketEncoder {

    private WebSocketEncoder webSocketEncoder = new WebSocketEncoder();
    private final int content = 666;

    @Test
    public void test() {
        ByteBuf byteMsg = Unpooled.buffer();
        byteMsg.writeByte(content);

        EmbeddedChannel channel = new EmbeddedChannel(webSocketEncoder);
        assertTrue(channel.writeOutbound(byteMsg));
        assertTrue(channel.finish());

        assertEquals(new BinaryWebSocketFrame(byteMsg), channel.readOutbound());
        assertNull(channel.readOutbound());
    }
}
