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

import io.netty.channel.socket.DatagramChannel;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapResponseCache;
import org.apache.rocketmq.mqtt.cs.session.infly.CoapRetryManager;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class DatagramChannelManager {

    @Resource
    private CoapResponseCache coapResponseCache;

    @Resource
    private CoapRetryManager coapRetryManager;

    private DatagramChannel channel;

    public void setChannel(DatagramChannel channel) {
        this.channel = channel;
    }

    public DatagramChannel getChannel() {
        return channel;
    }

    public void write(CoapMessage message) {
        channel.writeAndFlush(message);
    }

    // Write to channel and add to response cache.
    public void writeResponse(CoapMessage message) {
        channel.writeAndFlush(message);
        coapResponseCache.put(message);
    }

    // Write to channel and add to retry manager.
    public void pushMessage(CoapSession session, CoapMessage message) {
        channel.writeAndFlush(message);
        if (message.getType() == CoapMessageType.CON) {
            coapRetryManager.addRetryMessage(session, message);
        }
    }
}
