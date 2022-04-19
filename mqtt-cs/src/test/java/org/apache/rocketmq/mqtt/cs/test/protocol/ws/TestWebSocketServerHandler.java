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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketServerHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static io.netty.handler.codec.DecoderResult.UNFINISHED;
import static io.netty.handler.codec.http.HttpMethod.CONNECT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class TestWebSocketServerHandler {

    private WebSocketServerHandler webSocketServerHandler = new WebSocketServerHandler();
    private EmbeddedChannel channel = new EmbeddedChannel();

    @Before
    public void setUp() throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(webSocketServerHandler);
    }

    @Test
    public void testHttpRequestUpgradeNull() {
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, CONNECT, "ws://localhost:8888/mqtt");
        channel.writeInbound(httpRequest);

        assertNull(channel.readInbound());
    }

    @Test
    public void testSendHttpResponse() {
        FullHttpRequest httpRequest = new DefaultFullHttpRequest(HTTP_1_1, CONNECT, "ws://localhost:8888/mqtt");
        httpRequest.setDecoderResult(UNFINISHED);
        channel.writeInbound(httpRequest);

        assertNull(channel.readInbound());
    }

    @Test
    public void testPongWebSocketFrame() {
        PongWebSocketFrame socketFrame = new PongWebSocketFrame();
        channel.writeInbound(socketFrame);

        assertNull(channel.readInbound());
    }
}
