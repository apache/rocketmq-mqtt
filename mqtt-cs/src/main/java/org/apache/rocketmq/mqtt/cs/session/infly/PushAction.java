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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;


@Component
public class PushAction {
    private static Logger logger = LoggerFactory.getLogger(PushAction.class);

    @Resource
    private MqttMsgId mqttMsgId;

    @Resource
    private RetryDriver retryDriver;

    @Resource
    private InFlyCache inFlyCache;

    @Resource
    private ConnectConf connectConf;


    public void messageArrive(Session session, Subscription subscription, Queue queue) {
        if (session == null) {
            return;
        }
        if (!connectConf.isOrder()) {
            List<Message> list = session.pendMessageList(subscription, queue);
            if (list != null && !list.isEmpty()) {
                for (Message message : list) {
                    message.setAck(0);
                    push(message, subscription, session, queue);
                }
            }
            return;
        }
        if (retryDriver.needRetryBefore(subscription, queue, session)) {
            return;
        }
        Message message = session.nextSendMessageByOrder(subscription, queue);
        if (message != null) {
            push(message, subscription, session, queue);
        }
    }

    public void push(Message message, Subscription subscription, Session session, Queue queue) {
        String clientId = session.getClientId();
        int mqttId = mqttMsgId.nextId(clientId);
        inFlyCache.getPendingDownCache().put(session.getChannelId(), mqttId, subscription, queue, message);
        try {
            if (session.isClean()) {
                if (message.getStoreTimestamp() > 0 &&
                        message.getStoreTimestamp() < session.getStartTime()) {
                    logger.warn("old msg:{},{},{},{}", session.getClientId(), message.getMsgId(),
                            message.getStoreTimestamp(), session.getStartTime());
                    rollNext(session, mqttId);
                    return;
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        }
        int qos = subscription.getQos();
        if (subscription.isP2p() && message.qos() != null) {
            qos = message.qos();
        }
        if (qos == 0) {
            write(session, message, mqttId, 0, subscription);
            rollNextByAck(session, mqttId);
        } else {
            retryDriver.mountPublish(mqttId, message, subscription.getQos(), ChannelInfo.getId(session.getChannel()), subscription);
            write(session, message, mqttId, qos, subscription);
        }
    }

    public void write(Session session, Message message, int mqttId, int qos, Subscription subscription) {
        Channel channel = session.getChannel();
        String owner = ChannelInfo.getOwner(channel);
        String clientId = session.getClientId();
        String topicName = message.getOriginTopic();
        String mqttRealTopic = message.getUserProperty(Message.extPropertyMqttRealTopic);
        if (StringUtils.isNotBlank(mqttRealTopic)) {
            topicName = mqttRealTopic;
        }
        if (StringUtils.isBlank(topicName)) {
            topicName = message.getFirstTopic();
        }
        boolean isP2P = TopicUtils.isP2P(TopicUtils.decode(topicName).getSecondTopic());
        if (!channel.isWritable()) {
            logger.error("UnWritable:{}", clientId);
            return;
        }
        Object data = MessageUtil.toMqttMessage(topicName, message.getPayload(), qos, mqttId);
        ChannelFuture writeFuture = session.getChannel().writeAndFlush(data);
        int bodySize = message.getPayload() != null ? message.getPayload().length : 0;
        writeFuture.addListener((ChannelFutureListener) future -> {
            if (subscription.isRetry()) {
                message.setRetry(message.getRetry() + 1);
                logger.warn("retryPush:{},{},{}", session.getClientId(), message.getMsgId(), message.getRetry());
            }
        });
    }

    public void rollNextByAck(Session session, int mqttId) {
        if (session == null) {
            return;
        }
        mqttMsgId.releaseId(mqttId, session.getClientId());
        InFlyCache.PendingDown pendingDown = inFlyCache.getPendingDownCache().get(session.getChannelId(), mqttId);
        if (pendingDown == null) {
            return;
        }
        rollNext(session, mqttId);
    }

    public void rollNext(Session session, int mqttId) {
        if (session == null) {
            return;
        }
        mqttMsgId.releaseId(mqttId, session.getClientId());
        InFlyCache.PendingDown pendingDown = inFlyCache.getPendingDownCache().remove(session.getChannelId(), mqttId);
        if (pendingDown == null) {
            return;
        }
        _rollNext(session, pendingDown);
    }

    public void rollNextNoWaitRetry(Session session, int mqttId) {
        if (session == null || session.isDestroyed()) {
            return;
        }
        InFlyCache.PendingDown pendingDown = inFlyCache.getPendingDownCache().get(session.getChannelId(), mqttId);
        if (pendingDown == null) {
            return;
        }
        _rollNext(session, pendingDown);
    }

    public void _rollNext(Session session, InFlyCache.PendingDown pendingDown) {
        Subscription subscription = pendingDown.getSubscription();
        Queue pendingQueue = pendingDown.getQueue();
        long pendingDownSeqId = pendingDown.getSeqId();

        if (!connectConf.isOrder()) {
            session.ack(subscription, pendingQueue, pendingDownSeqId);
            return;
        }

        Message nextSendOne = session.rollNext(subscription, pendingQueue, pendingDownSeqId);
        if (nextSendOne != null) {
            push(nextSendOne, subscription, session, pendingQueue);
        }
    }

}
