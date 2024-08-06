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

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class CoapRetryManager {

    @Resource
    private DatagramChannelManager datagramChannelManager;

    private static Logger logger = LoggerFactory.getLogger(CoapRetryManager.class);

    private ScheduledThreadPoolExecutor scheduler;

    private ConcurrentMap<Integer, RetryMessage> retryMessageMap = new ConcurrentHashMap<>(1024);

    private static final int SCHEDULE_INTERVAL = 1000;
    private static final int MAX_RETRY_TIME = 3;
    private static final long RETRY_INTERVAL = 3000;

    @PostConstruct
    public void init() {
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("coap_retry_message_thread_"));
        scheduler.scheduleWithFixedDelay(() -> doRetry(), SCHEDULE_INTERVAL, SCHEDULE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public void addRetryMessage(CoapSession session, CoapMessage message) {
        retryMessageMap.put(message.getMessageId(), new RetryMessage(message.getMessageId(), message, session));
    }

    public void removeRetryMessage(int messageId) {
        RetryMessage removedMessage = retryMessageMap.remove(messageId);
        // refresh subscription each time receiving an ACK
        if (removedMessage.session != null) {
            removedMessage.session.refreshSubscribeTime();
        }
    }

    public boolean contains(int messageId) {
        return retryMessageMap.containsKey(messageId);
    }

    private void doRetry() {
        if (retryMessageMap.isEmpty()) {
            return;
        }
        for (RetryMessage retryMessage : retryMessageMap.values()) {
            if (System.currentTimeMillis() - retryMessage.lastSendTime < RETRY_INTERVAL) {
                continue;
            }
            if (retryMessage.retryTime >= MAX_RETRY_TIME) {
                removeRetryMessage(retryMessage.messageId);
                logger.info("coap retry message expired, messageId:{}", retryMessage.messageId);
                continue;
            }
            // update messageID if session has newer messageID
            CoapSession session = retryMessage.session;
            int latestMessageNum = session.getMessageNum();
            int latestMessageID = session.getMessageId() + latestMessageNum;
            if (latestMessageID > retryMessage.messageId) {
                retryMessage.messageId = latestMessageID;
                retryMessage.message.setMessageId(latestMessageID);
                retryMessage.message.clearOptions();
                retryMessage.message.addObserveOption(latestMessageNum);
                session.messageNumIncrement();
            }
            datagramChannelManager.write(retryMessage.message);
            retryMessage.retryTime++;
            retryMessage.lastSendTime = System.currentTimeMillis();
        }
    }

    public class RetryMessage {
        private int messageId;
        private CoapMessage message;
        private CoapSession session;
        private int retryTime = 0;
        private long lastSendTime = System.currentTimeMillis();

        public RetryMessage(int messageId, CoapMessage message) {
            this.messageId = messageId;
            this.message = message;
        }

        public RetryMessage(int messageId, CoapMessage message, CoapSession session) {
            this.messageId = messageId;
            this.message = message;
            this.session = session;
        }

    }
}
