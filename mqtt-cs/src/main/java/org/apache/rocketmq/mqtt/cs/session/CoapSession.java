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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapMessageOption;
import org.apache.rocketmq.mqtt.common.model.CoapMessageOptionNumber;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class CoapSession {
    private static Logger logger = LoggerFactory.getLogger(CoapSession.class);
    private InetSocketAddress address;
    private ChannelHandlerContext ctx;
    private int messageId;
    private byte[] token;
    private int messageNum = 0;
    private long subscribeTime;
    private volatile int pullSize;

    private Subscription subscription;
    private Map<Queue, QueueOffset> offsetMap = new ConcurrentHashMap<>(16);
    Map<Queue, LinkedHashSet<Message>> sendingMessages = new ConcurrentHashMap<>(16);

    public CoapSession() {}

    public void refreshSubscribeTime() {
        this.subscribeTime = System.currentTimeMillis();
    }

    public QueueOffset getQueueOffset(Queue queue) {
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        return offsetMap.get(queue);
    }

    public void freshQueue(Set<Queue> queues) {
        if (this.subscription == null) {
            throw new RuntimeException("subscription is null");
        }
        if (queues == null) {
            logger.warn("queues is null when freshQueue,{},{}", this.address, this.subscription);
            return;
        }

        for (Queue memQueue: offsetMap.keySet()) {
            if (!queues.contains(memQueue)) {
                offsetMap.remove(memQueue);
            }
        }

        // init queueOffset
        for (Queue nowQueue : queues) {
            if (!offsetMap.containsKey(nowQueue)) {
                QueueOffset queueOffset = new QueueOffset();
                offsetMap.put(nowQueue, queueOffset);
                // todo: this.markPersistOffsetFlag(true);
            }
        }

        for (Queue memQueue : sendingMessages.keySet()) {
            if (!queues.contains(memQueue)) {
                sendingMessages.remove(memQueue);
            }
        }

        if (queues.isEmpty()) {
            logger.warn("queues is empty when freshQueue,{},{}", this.address, this.subscription);
        }
    }

    public void addOffset(Queue queue, QueueOffset offset) {
        offsetMap.put(queue, offset);
    }

    public void updateQueueOffset(Queue queue, Message message) {
        if (!offsetMap.containsKey(queue)) {
            logger.warn("failed update queue offset,not found queueOffset,{},{},{}", this.address, this.subscription,
                    queue);
            return;
        }
        QueueOffset queueOffset = offsetMap.get(queue);
        queueOffset.setOffset(message.getOffset() + 1);
    }

    public boolean addSendingMessages(Queue queue, List<Message> messages) {
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        if (messages == null || messages.isEmpty()) {
            return false;
        }
        if (subscription.isShare()) {
            return true;
        }
        if (!sendingMessages.containsKey(queue)) {
            sendingMessages.putIfAbsent(queue, new LinkedHashSet<>(8));
        }
        if (!offsetMap.containsKey(queue)) {
            logger.warn("not found queueOffset,{},{},{}", this.address, this.subscription, queue);
            return false;
        }
        boolean add =false;
        QueueOffset queueOffset = offsetMap.get(queue);
        for (Message message : messages) {
            if (message.getOffset() < queueOffset.getOffset() && queueOffset.getOffset() != Long.MAX_VALUE) {
                continue;
            }
            synchronized (this) {
                if (sendingMessages.get(queue).add(message.copy())) {
                    add = true;
                }
            }
        }
        return add;
    }

    public boolean sendingMessageIsEmpty(Queue queue) {
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        LinkedHashSet<Message> messages = sendingMessages.get(queue);
        if (messages == null) {
            return true;
        }
        synchronized (this) {
            return messages.isEmpty();
        }
    }

    public List<Message> pendMessageList(Queue queue) {
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        List<Message> list = new ArrayList<>();
        LinkedHashSet<Message> messages = sendingMessages.get(queue);
        if (messages == null) {
            return null;
        }
        synchronized (this) {
            if (!messages.isEmpty()) {
                for (Message message : messages) {
                    if (message.getAck() == -1) {
                        list.add(message);
                    }
                }
            }
        }
        return list;
    }

    public void ack(Queue pendingQueue, long pendingDownSeqId) {
        if (pendingQueue == null) {
            throw new RuntimeException("queue is null");
        }
        LinkedHashSet<Message> messages = sendingMessages.get(pendingQueue);
        if (messages == null) {
            return;
        }
        synchronized (this) {
            if (messages.isEmpty()) {
                return;
            }
            boolean flag = true;
            Iterator<Message> iterator = messages.iterator();
            while (iterator.hasNext()) {
                Message message = iterator.next();
                if (message.getOffset() == pendingDownSeqId) {
                    message.setAck(1);
                }
                if (message.getAck() != 1) {
                    flag = false;
                }
                if (flag) {
                    updateQueueOffset(pendingQueue, message);
//                    this.markPersistOffsetFlag(true);
                    iterator.remove();
                }
            }
        }
    }

    public Message nextSendMessageByOrder(Queue queue) {
        if (queue == null) {
            throw new RuntimeException("queue is null");
        }
        LinkedHashSet<Message> messages = sendingMessages.get(queue);
        if (messages == null) {
            return null;
        }
        synchronized (this) {
            return messages.isEmpty() ? null : messages.iterator().next();
        }
    }

    public CoapMessage sendNewMessage(Queue queue, Message messageSend) {
        CoapMessage data = write(messageSend.getPayload());
        LinkedHashSet<Message> messages = sendingMessages.get(queue);
        if (messages == null) {
            return null;
        }
        synchronized (this) {
            if (messages.isEmpty()) {
                return null;
            }
            Iterator<Message> iterator = messages.iterator();
            while (iterator.hasNext()) {
                Message message = iterator.next();
                if (message.equals(messageSend)) {
                    message.setAck(1);
                }
                if (message.getAck() == 1) {
                    updateQueueOffset(queue, message);
                    iterator.remove();
                }
            }
        }
        return data;
    }

    public void sendRemoveSessionMessage() {
        byte[] payload = "Subscription is expired, please subscribe again.".getBytes(StandardCharsets.UTF_8);
        write(payload, CoapMessageCode.FORBIDDEN);
    }

    public CoapMessage write(byte[] payload) {
        return write(payload, CoapMessageCode.CONTENT);
    }

    public CoapMessage write(byte[] payload, CoapMessageCode code) {
        this.messageNum++;
        CoapMessage data = new CoapMessage(
                Constants.COAP_VERSION,
                this.subscription.getQos() == 0 ? CoapMessageType.NON : CoapMessageType.CON,
                this.token.length,
                code,
                this.messageId + this.messageNum,
                this.token,
                payload,
                this.address
        );
        data.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, intToByteArray(this.messageNum)));
        if (this.ctx.channel().isActive()) {
            this.ctx.writeAndFlush(data);
        } else {
            System.out.println("Channel is not active");
        }
        return data;
    }

    private byte[] intToByteArray(int value) {
        byte[] byteArray = new byte[3];
        byteArray[0] = (byte) (value >> 16);
        byteArray[1] = (byte) (value >> 8);
        byteArray[2] = (byte) (value);
        return byteArray;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public void setAddress(InetSocketAddress address) {
        this.address = address;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
    }

    public int getMessageNum() {
        return messageNum;
    }

    public void setMessageNum(int messageNum) {
        this.messageNum = messageNum;
    }

    public long getSubscribeTime() {
        return subscribeTime;
    }

    public void setSubscribeTime(long subscribeTime) {
        this.subscribeTime = subscribeTime;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }

    public int getPullSize() {
        return pullSize;
    }

    public void setPullSize(int pullSize) {
        this.pullSize = pullSize;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public Map<Queue, QueueOffset> getOffsetMap() {
        return offsetMap;
    }

    public void setOffsetMap(Map<Queue, QueueOffset> offsetMap) {
        this.offsetMap = offsetMap;
    }
}
