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

package org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.MqttPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.facotry.MqttMessageFactory;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.loop.WillLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.RETAIN_AVAILABLE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SHARED_SUBSCRIPTION_AVAILABLE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.USER_PROPERTY;

@Component
public class Mqtt5ConnectHandler implements MqttPacketHandler<MqttConnectMessage> {
    private static Logger logger = LoggerFactory.getLogger(Mqtt5ConnectHandler.class);

    private static final Integer DEFAULT_SESSION_EXPIRY_INTERVAL = 0;
    private static final Integer DEFAULT_RECEIVE_MAXIMUM = 65535;
    private static final Integer DEFAULT_TOPIC_ALIAS_MAXIMUM = 0;
    private static final Integer DEFAULT_REQUEST_RESPONSE_INFORMATION = 0;
    private static final Integer DEFAULT_REQUEST_PROBLEM_INFORMATION = 1;


    @Resource
    private ChannelManager channelManager;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private WillLoop willLoop;

    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("check_connect_future"));

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, MqttConnectMessage connectMessage) {
        final MqttConnectVariableHeader variableHeader = connectMessage.variableHeader();
        MqttProperties mqttProperties = variableHeader.properties();
        Channel channel = ctx.channel();

        if (mqttProperties.getProperty(RECEIVE_MAXIMUM.value()) != null &&
                ((MqttProperties.IntegerProperty) mqttProperties.getProperty(RECEIVE_MAXIMUM.value())).value() == 0) {
            channelManager.closeConnectWithProtocolError(channel);
            return false;
        }

        if (mqttProperties.getProperty(MAXIMUM_PACKET_SIZE.value()) != null &&
                ((MqttProperties.IntegerProperty) mqttProperties.getProperty(MAXIMUM_PACKET_SIZE.value())).value() == 0) {
            channelManager.closeConnectWithProtocolError(channel);
            return false;
        }


        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, MqttConnectMessage connectMessage, HookResult upstreamHookResult) {
        final MqttConnectVariableHeader variableHeader = connectMessage.variableHeader();
        final MqttConnectPayload payload = connectMessage.payload();

        Channel channel = ctx.channel();
        ChannelInfo.setKeepLive(channel, variableHeader.keepAliveTimeSeconds());
        ChannelInfo.setClientId(channel, connectMessage.payload().clientIdentifier());
        ChannelInfo.setCleanSessionFlag(channel, variableHeader.isCleanSession());

        MqttProperties mqttProperties = variableHeader.properties();

        Integer sessionExpiryInterval = DEFAULT_SESSION_EXPIRY_INTERVAL;
        if (mqttProperties.getProperty(SESSION_EXPIRY_INTERVAL.value()) != null) {
            sessionExpiryInterval =
                    Math.max(0, ((MqttProperties.IntegerProperty) mqttProperties.getProperty(SESSION_EXPIRY_INTERVAL.value())).value());
        }
        ChannelInfo.setSessionExpiryInterval(channel, sessionExpiryInterval);

        Integer receiveMaximum = DEFAULT_RECEIVE_MAXIMUM;
        if (mqttProperties.getProperty(RECEIVE_MAXIMUM.value()) != null) {
            receiveMaximum = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(RECEIVE_MAXIMUM.value())).value();
        }
        ChannelInfo.setReceiveMaximum(channel, receiveMaximum);

        // MAXIMUM_PACKET_SIZE no default value. If the Maximum Packet Size is not present, no limit
        if (mqttProperties.getProperty(MAXIMUM_PACKET_SIZE.value()) != null) {
            ChannelInfo.setMaximumPacketSize(channel,
                    ((MqttProperties.IntegerProperty) mqttProperties.getProperty(MAXIMUM_PACKET_SIZE.value())).value());
        }

        Integer topicAliasMaximum = DEFAULT_TOPIC_ALIAS_MAXIMUM;
        if (mqttProperties.getProperty(TOPIC_ALIAS_MAXIMUM.value()) != null) {
            topicAliasMaximum = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(TOPIC_ALIAS_MAXIMUM.value())).value();
        }
        ChannelInfo.setTopicAliasMaximum(channel, topicAliasMaximum);

        Integer requestResponseInformation = DEFAULT_REQUEST_RESPONSE_INFORMATION;
        if (mqttProperties.getProperty(REQUEST_PROBLEM_INFORMATION.value()) != null) {
            requestResponseInformation = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(REQUEST_PROBLEM_INFORMATION.value())).value();
        }
        ChannelInfo.setRequestResponseInformation(channel, requestResponseInformation);

        Integer requestProblemInformation = DEFAULT_REQUEST_PROBLEM_INFORMATION;
        if (mqttProperties.getProperty(REQUEST_PROBLEM_INFORMATION.value()) != null) {
            requestProblemInformation = ((MqttProperties.IntegerProperty) mqttProperties.getProperty(REQUEST_PROBLEM_INFORMATION.value())).value();
        }
        ChannelInfo.setRequestProblemInformation(channel, requestProblemInformation);

        if (mqttProperties.getProperties(USER_PROPERTY.value()) != null) {
            ChannelInfo.setUserProperty(channel, (List<MqttProperties.UserProperty>) mqttProperties.getProperties(USER_PROPERTY.value()));
        }

        String remark = upstreamHookResult.getRemark();
        if (!upstreamHookResult.isSuccess()) {
            byte connAckCode = (byte) upstreamHookResult.getSubCode();
            MqttConnectReturnCode mqttConnectReturnCode = MqttConnectReturnCode.valueOf(connAckCode);
            channel.writeAndFlush(MqttMessageFactory.buildConnAckMessage(mqttConnectReturnCode));
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, remark);
            return;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ChannelInfo.setFuture(channel, ChannelInfo.FUTURE_CONNECT, future);

        // use 'scheduler' to separate two i/o: 'ack to client' and 'session-load from rocketmq'
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        }, 1, TimeUnit.SECONDS);

        try {
            Boolean sessionPresent = false;
            if (!ChannelInfo.getCleanSessionFlag(channel) && sessionLoop.getSessionList(ChannelInfo.getClientId(channel)).size() > 0) {
                sessionPresent = true;
            }

            // set server properties send to client
            MqttProperties props = new MqttProperties();
            props.add(new MqttProperties.IntegerProperty(RETAIN_AVAILABLE.value(), 0));
            props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER_AVAILABLE.value(), 0));
            props.add(new MqttProperties.IntegerProperty(SHARED_SUBSCRIPTION_AVAILABLE.value(), 0));

            final MqttConnAckMessage mqttConnAckMessage = MqttMessageFactory.createConnAckMessage(
                    MqttConnectReturnCode.CONNECTION_ACCEPTED,
                    sessionPresent,
                    props);

            future.thenAccept(aVoid -> {
                if (!channel.isActive()) {
                    return;
                }
                ChannelInfo.removeFuture(channel, ChannelInfo.FUTURE_CONNECT);
                channel.writeAndFlush(mqttConnAckMessage);
            });

            // TODO if sessionPresent = true, should load the exist session by clientId
            sessionLoop.loadSession(ChannelInfo.getClientId(channel), channel);

            // TODO save will Propertiess in MQTT 5.0
            // save will message
            WillMessage willMessage = null;
            if (variableHeader.isWillFlag()) {
                if (payload.willTopic() == null || payload.willMessageInBytes() == null) {
                    logger.error("Will message and will topic can not be empty");
                    channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "Will message and will topic can not be empty");
                    return;
                }

                willMessage = new WillMessage(payload.willTopic(), payload.willMessageInBytes(), variableHeader.isWillRetain(), variableHeader.willQos());
                willLoop.addWillMessage(channel, willMessage);
            }

        } catch (Exception e) {
            logger.error("Connect:{}", payload.clientIdentifier(), e);
            channelManager.closeConnect(channel, ChannelCloseFrom.SERVER, "ConnectException");
        }
    }
}
