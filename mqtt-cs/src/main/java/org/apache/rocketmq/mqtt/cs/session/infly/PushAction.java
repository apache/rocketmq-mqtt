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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.model.CoapMessage;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CONTENT_TYPE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.CORRELATION_DATA;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RESPONSE_TOPIC;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
import static java.lang.Math.min;
import static java.util.Objects.hash;


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

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private DatagramChannelManager datagramChannelManager;

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

    public void coapMessageArrive(CoapSession session, Queue queue) {
        if (session == null) {
            return;
        }
        List<Message> list = session.pendMessageList(queue);
        if (list != null && !list.isEmpty()) {
            for (Message message : list) {
                message.setAck(0);
                coapPush(message, session, queue);
            }
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

        //deal with message with empty payload
        String msgPayLoad = new String(message.getPayload());
        if (msgPayLoad.equals(MessageUtil.EMPTYSTRING) && message.isEmpty()) {
            message.setPayload("".getBytes());
        }

        int qos = subscription.getQos();
        if (message.qos() != null && (subscription.isP2p() || message.qos() < qos)) {
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

    public void coapPush(Message message, CoapSession session, Queue queue) {
        try {
            if (message.getStoreTimestamp() > 0 && message.getStoreTimestamp() < session.getSubscribeTime()) {
                logger.warn("coap old msg:{},{},{},{}", session.getAddress(), message.getMsgId(),
                        message.getStoreTimestamp(), session.getSubscribeTime());
                return;
            }
        } catch (Exception e) {
            logger.error("", e);
        }

        // Deal with message with empty payload
        String msgPayLoad = new String(message.getPayload());
        if (msgPayLoad.equals(MessageUtil.EMPTYSTRING) && message.isEmpty()) {
            message.setPayload("".getBytes());
        }

        // Send message to client, and remove from the sendingMessages of session.
        session.messageNumIncrement();
        CoapMessage sendMessage = buildCoapMessage(message, session);
        datagramChannelManager.pushMessage(session, sendMessage);
        session.ack(queue, message);
    }

    public void _sendMessage(Session session, String clientId, Subscription subscription, Message message) {

        String payLoad = new String(message.getPayload());
        if (payLoad.equals(MessageUtil.EMPTYSTRING) && message.isEmpty()) {
            return;
        }

        int mqttId = mqttMsgId.nextId(clientId);
        int qos = min(subscription.getQos(), message.qos());
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
        boolean retained = message.isRetained();
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
        Object data = null;

        MqttVersion mqttVersion = ChannelInfo.getMqttVersion(channel);
        MqttProperties mqttProperties = new MqttProperties();
        switch (mqttVersion) {
            case MQTT_3_1_1:
            case MQTT_3_1:
                data = MqttMessageFactory.buildPublishMessage(topicName, message.getPayload(), qos, retained, mqttId);
                break;
            case MQTT_5:
                // Server send PublishMessage flow control
                if (qos > 0 && !session.publishSendTryAcquire()) {
                    logger.error("publishSendTryAcquire failed, client:{}, message:{}", clientId, message);
                    return;
                }

                // add content type
                if (StringUtils.isNotBlank(message.getUserProperty(Message.propertyContentType))) {
                    mqttProperties.add(new MqttProperties.StringProperty(CONTENT_TYPE.value(), message.getUserProperty(Message.propertyContentType)));
                }

                // add Response Topic
                if (StringUtils.isNotBlank(message.getUserProperty(Message.propertyResponseTopic))) {
                    mqttProperties.add(new MqttProperties.StringProperty(RESPONSE_TOPIC.value(), message.getUserProperty(Message.propertyResponseTopic)));
                }

                // add Correlation Data
                if (StringUtils.isNotBlank(message.getUserProperty(Message.propertyCorrelationData))) {
                    mqttProperties.add(new MqttProperties.StringProperty(CORRELATION_DATA.value(), message.getUserProperty(Message.propertyCorrelationData)));
                }

                // process publish user properties
                processUserProperties(message, mqttProperties);

                // process subscription identifier
                if (subscription.getSubscriptionIdentifier() > 0) {
                    mqttProperties.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), subscription.getSubscriptionIdentifier()));
                }

                // TODO retain flag should be set by subscription option
                boolean isRetained = message.isRetained();

                // process topic alias
                if (!processTopicAlias(channel, topicName, mqttProperties)) {
                    data = MqttMessageFactory.buildMqtt5PublishMessage("", message.getPayload(), qos, isRetained, mqttId, mqttProperties);
                } else {
                    data = MqttMessageFactory.buildMqtt5PublishMessage(topicName, message.getPayload(), qos, isRetained, mqttId, mqttProperties);
                }
                break;
            default:
                break;
        }

        ChannelFuture writeFuture = session.getChannel().writeAndFlush(data);
        int bodySize = message.getPayload() != null ? message.getPayload().length : 0;
        writeFuture.addListener((ChannelFutureListener) future -> {
            if (subscription.isRetry()) {
                message.setRetry(message.getRetry() + 1);
                logger.warn("retryPush:{},{},{}", session.getClientId(), message.getMsgId(), message.getRetry());
            } else if (subscription.isShare()) {
                String lmqTopic = MixAll.LMQ_PREFIX + StringUtils.replace(message.getOriginTopic(), "/", "%");
                lmqQueueStore.popAck(lmqTopic, subscription.getSharedName(), message);
            }
        });
    }

    public void processUserProperties(Message message, MqttProperties mqttProperties) {
        String mqtt5UserProperties = message.getUserProperty(Message.propertyMqtt5UserProperty);
        if (StringUtils.isNotBlank(mqtt5UserProperties)) {
            ArrayList<MqttProperties.StringPair> userProperties = JSON.parseObject(mqtt5UserProperties,
                    new TypeReference<ArrayList<MqttProperties.StringPair>>() {
                    }
            );
            mqttProperties.add(new MqttProperties.UserProperties(userProperties));
        }
    }

    /**
     * process topic alias
     * @param channel
     * @param topicName
     * @param mqttProperties
     * @return true: conflict when allocated topic alias
     */
    public boolean processTopicAlias(Channel channel, String topicName, MqttProperties mqttProperties) {
        int topicAlias = ChannelInfo.getTopicAliasMaximum(channel);
        boolean conflict = false;

        if (topicAlias > 0) {
            if (ChannelInfo.getServerTopicAlias(channel, topicName) == null) {
                // allocate topic alias
                int allocateAlias = genServerTopicAlias(topicName, topicAlias);

                if (ChannelInfo.getServerAliasTopic(channel, allocateAlias) != null) {
                    // conflict, client will reset topic <-> alias
                    conflict = true;
                }

                ChannelInfo.setServerTopicAlias(channel, topicName, allocateAlias);
                ChannelInfo.setServerAliasTopic(channel, allocateAlias, topicName);
            }

            // topic has allocated topic alias,just set to mqttProperties
            mqttProperties.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), ChannelInfo.getServerTopicAlias(channel, topicName)));
        }
        return conflict;
    }

    public int genServerTopicAlias(String topicName, int topicAliasMaximum) {
        return hash(topicName) % topicAliasMaximum + 1;
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

    private CoapMessage buildCoapMessage(Message message, CoapSession session) {
        CoapMessage coapMessage = new CoapMessage(
                Constants.COAP_VERSION,
                session.getSubscription().getQos() == 0 ? CoapMessageType.NON : CoapMessageType.CON,
                session.getToken().length,
                CoapMessageCode.CONTENT,
                session.getMessageId() + session.getMessageNum(),
                session.getToken(),
                message.getPayload(),
                session.getAddress()
        );
        coapMessage.addObserveOption(session.getMessageNum());
        return coapMessage;
    }

}
