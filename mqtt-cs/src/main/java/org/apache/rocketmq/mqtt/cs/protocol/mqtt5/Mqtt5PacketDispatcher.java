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

package org.apache.rocketmq.mqtt.cs.protocol.mqtt5;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.ReferenceCountUtil;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.util.HostInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelDecodeException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5AuthHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5ConnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5DisconnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PingHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PubAckHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PubCompHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PubRecHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PubRelHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PublishHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5SubscribeHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5UnSubscribeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.mqtt.MqttMessageType.PUBLISH;


@ChannelHandler.Sharable
@Component
public class Mqtt5PacketDispatcher extends SimpleChannelInboundHandler<MqttMessage> {
    private static Logger logger = LoggerFactory.getLogger(Mqtt5PacketDispatcher.class);

    @Resource
    private Mqtt5ConnectHandler mqtt5ConnectHandler;

    @Resource
    private Mqtt5DisconnectHandler mqtt5DisconnectHandler;

    @Resource
    private Mqtt5PublishHandler mqtt5PublishHandler;

    @Resource
    private Mqtt5SubscribeHandler mqtt5SubscribeHandler;

    @Resource
    private Mqtt5PubAckHandler mqtt5PubAckHandler;

    @Resource
    private Mqtt5PingHandler mqtt5PingHandler;

    @Resource
    private Mqtt5UnSubscribeHandler mqtt5UnSubscribeHandler;

    @Resource
    private Mqtt5PubRelHandler mqtt5PubRelHandler;

    @Resource
    private Mqtt5PubRecHandler mqtt5PubRecHandler;

    @Resource
    private Mqtt5PubCompHandler mqtt5PubCompHandler;

    @Resource
    private Mqtt5AuthHandler mqtt5AuthHandler;

    @Resource
    private UpstreamHookManager upstreamHookManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!ctx.channel().isActive()) {
            return;
        }
        if (!msg.decoderResult().isSuccess()) {
            throw new ChannelDecodeException(ChannelInfo.getClientId(ctx.channel()) + "," + msg.decoderResult());
        }
        ChannelInfo.touch(ctx.channel());
        boolean preResult = preHandler(ctx, msg);
        if (!preResult) {
            return;
        }
        CompletableFuture<HookResult> upstreamHookResult;
        try {
            if (msg instanceof MqttPublishMessage) {
                ((MqttPublishMessage) msg).retain();
            }
            upstreamHookResult = upstreamHookManager.doUpstreamHook(buildMqttMessageUpContext(ctx), msg);
            if (upstreamHookResult == null) {
                _channelRead0(ctx, msg, null);
                return;
            }
        } catch (Throwable t) {
            logger.error("", t);
            if (msg instanceof MqttPublishMessage) {
                ReferenceCountUtil.release(msg);
            }
            throw new ChannelException(t.getMessage());
        }
        upstreamHookResult.whenComplete((hookResult, throwable) -> {
            if (msg instanceof MqttPublishMessage) {
                ReferenceCountUtil.release(msg);
            }
            if (throwable != null) {
                logger.error("", throwable);
                ctx.fireExceptionCaught(new ChannelException(throwable.getMessage()));
                return;
            }
            if (hookResult == null) {
                ctx.fireExceptionCaught(new ChannelException("UpstreamHook Result Unknown"));
                return;
            }
            try {
                _channelRead0(ctx, msg, hookResult);
            } catch (Throwable t) {
                logger.error("", t);
                ctx.fireExceptionCaught(new ChannelException(t.getMessage()));
            }
        });
    }

    private void _channelRead0(ChannelHandlerContext ctx, MqttMessage msg, HookResult upstreamHookResult) {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                mqtt5ConnectHandler.doHandler(ctx, (MqttConnectMessage) msg, upstreamHookResult);
                break;
            case PUBLISH:
                mqtt5PublishHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBACK:
                mqtt5PubAckHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBREC:
                mqtt5PubRecHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBREL:
                mqtt5PubRelHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBCOMP:
                mqtt5PubCompHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case SUBSCRIBE:
                mqtt5SubscribeHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case UNSUBSCRIBE:
                mqtt5UnSubscribeHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PINGREQ:
                mqtt5PingHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case DISCONNECT:
                mqtt5DisconnectHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case AUTH:
                mqtt5AuthHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            default:
        }
    }

    private boolean preHandler(ChannelHandlerContext ctx, MqttMessage msg) {
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                return mqtt5ConnectHandler.preHandler(ctx, (MqttConnectMessage) msg);
            case PUBLISH:
                return mqtt5PublishHandler.preHandler(ctx, msg);
            case SUBSCRIBE:
                return mqtt5SubscribeHandler.preHandler(ctx, msg);
            case PUBACK:
                return mqtt5PubAckHandler.preHandler(ctx, msg);
            case PINGREQ:
                return mqtt5PingHandler.preHandler(ctx, msg);
            case UNSUBSCRIBE:
                return mqtt5UnSubscribeHandler.preHandler(ctx, msg);
            case PUBREL:
                return mqtt5PubRelHandler.preHandler(ctx, msg);
            case PUBREC:
                return mqtt5PubRecHandler.preHandler(ctx, msg);
            case PUBCOMP:
                return mqtt5PubCompHandler.preHandler(ctx, msg);
            case DISCONNECT:
                return mqtt5DisconnectHandler.preHandler(ctx, msg);
            default:
                return true;
        }
    }

    public MqttMessageUpContext buildMqttMessageUpContext(ChannelHandlerContext ctx) {
        MqttMessageUpContext context = new MqttMessageUpContext();
        Channel channel = ctx.channel();
        context.setClientId(ChannelInfo.getClientId(channel));
        context.setChannelId(ChannelInfo.getId(channel));
        context.setNode(HostInfo.getInstall().getAddress());
        context.setNamespace(ChannelInfo.getNamespace(channel));
        return context;
    }

}
