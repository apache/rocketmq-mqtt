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
package org.apache.rocketmq.mqtt.cs.session;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class CoapTokenManager {

    private ScheduledThreadPoolExecutor scheduler;

    private ConcurrentMap<String, CoapToken> tokenMap = new ConcurrentHashMap<>(1024);

    private static final int SCHEDULE_INTERVAL = 1000;
    private static final long TIMEOUT = 3000;

    @PostConstruct
    public void init() {
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("coap_token_alive_thread_"));
        scheduler.scheduleWithFixedDelay(this::clearExpiredToken, SCHEDULE_INTERVAL, SCHEDULE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public String createToken(String clientId) {
        CoapToken coapToken = new CoapToken(clientId);
        tokenMap.put(clientId, coapToken);
        return coapToken.token;
    }

    public void removeToken(String clientId) {
        tokenMap.remove(clientId);
    }

    public String getToken(String clientId) {
        CoapToken coapToken = tokenMap.get(clientId);
        return coapToken == null ? null : coapToken.token;
    }

    public boolean isValid(String clientId, String token) {
        CoapToken coapToken = tokenMap.get(clientId);
        if (coapToken == null) {
            return false;
        }
        return coapToken.token.equals(token);
    }

    public void refreshToken(String clientId) {
        CoapToken coapToken = tokenMap.get(clientId);
        if (coapToken != null) {
            coapToken.lastUpdateTime = System.currentTimeMillis();
        }
    }

    private void clearExpiredToken() {
        if (tokenMap.isEmpty()) {
            return;
        }
        for (CoapToken token : tokenMap.values()) {
            if (System.currentTimeMillis() - token.lastUpdateTime > TIMEOUT) {
                tokenMap.remove(token.clientId);
            }
        }
    }

    public class CoapToken {
        private String clientId;
        private String token = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        private long lastUpdateTime = System.currentTimeMillis();

        public CoapToken(String clientId) {
            this.clientId = clientId;
        }
    }

}
