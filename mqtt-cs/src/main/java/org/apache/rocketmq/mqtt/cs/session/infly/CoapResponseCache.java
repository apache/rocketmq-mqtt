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
package org.apache.rocketmq.mqtt.cs.session.infly;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.springframework.stereotype.Component;

@Component
public class CoapResponseCache {
    private static final int MAX_SIZE = 10000;
    private Cache<Integer, CoapMessage> responseCache = Caffeine.newBuilder().maximumSize(MAX_SIZE).build();

    public void put(CoapMessage coapMessage) {
        responseCache.put(coapMessage.getMessageId(), coapMessage);
    }

    public CoapMessage get(int messageId) {
        return responseCache.getIfPresent(messageId);
    }

}
