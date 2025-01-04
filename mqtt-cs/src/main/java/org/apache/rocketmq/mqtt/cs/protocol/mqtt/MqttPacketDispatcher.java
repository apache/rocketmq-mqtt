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

package org.apache.rocketmq.mqtt.cs.protocol.mqtt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.ReferenceCountUtil;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.common.util.HostInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelDecodeException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelException;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttConnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttDisconnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPingHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubAckHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubCompHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubRecHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPubRelHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPublishHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttUnSubscribeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;


@ChannelHandler.Sharable
@Component
public class MqttPacketDispatcher extends SimpleChannelInboundHandler<MqttMessage> {
    private static Logger logger = LoggerFactory.getLogger(MqttPacketDispatcher.class);

    @Resource
    private MqttConnectHandler mqttConnectHandler;

    @Resource
    private MqttDisconnectHandler mqttDisconnectHandler;

    @Resource
    private MqttPublishHandler mqttPublishHandler;

    @Resource
    private MqttSubscribeHandler mqttSubscribeHandler;

    @Resource
    private MqttPubAckHandler mqttPubAckHandler;

    @Resource
    private MqttPingHandler mqttPingHandler;

    @Resource
    private MqttUnSubscribeHandler mqttUnSubscribeHandler;

    @Resource
    private MqttPubRelHandler mqttPubRelHandler;

    @Resource
    private MqttPubRecHandler mqttPubRecHandler;

    @Resource
    private MqttPubCompHandler mqttPubCompHandler;

    @Resource
    private UpstreamHookManager upstreamHookManager;

    @Resource
    private ChannelManager channelManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
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
                mqttConnectHandler.doHandler(ctx, (MqttConnectMessage) msg, upstreamHookResult);
                break;
            case PUBLISH:
                mqttPublishHandler.doHandler(ctx, (MqttPublishMessage) msg, upstreamHookResult);
                break;
            case SUBSCRIBE:
                mqttSubscribeHandler.doHandler(ctx, (MqttSubscribeMessage) msg, upstreamHookResult);
                break;
            case PUBACK:
                mqttPubAckHandler.doHandler(ctx, (MqttPubAckMessage) msg, upstreamHookResult);
                break;
            case PINGREQ:
                mqttPingHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case UNSUBSCRIBE:
                mqttUnSubscribeHandler.doHandler(ctx, (MqttUnsubscribeMessage) msg, upstreamHookResult);
                break;
            case PUBREL:
                mqttPubRelHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBREC:
                mqttPubRecHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case PUBCOMP:
                mqttPubCompHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            case DISCONNECT:
                mqttDisconnectHandler.doHandler(ctx, msg, upstreamHookResult);
                break;
            default:
        }
    }

    private boolean preHandler(ChannelHandlerContext ctx, MqttMessage msg) {
        if (!msg.fixedHeader().messageType().equals(MqttMessageType.CONNECT)) {
            Boolean connected = ctx.channel().attr(ChannelInfo.CHANNEL_CONNECTED_ATTRIBUTE_KEY).get();
            if (connected == null || !connected) {
                channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, Remark.NOT_CONNECTED);
                return false;
            }
        }
        switch (msg.fixedHeader().messageType()) {
            case CONNECT:
                return mqttConnectHandler.preHandler(ctx, (MqttConnectMessage) msg);
            case PUBLISH:
                return mqttPublishHandler.preHandler(ctx, (MqttPublishMessage) msg);
            case SUBSCRIBE:
                return mqttSubscribeHandler.preHandler(ctx, (MqttSubscribeMessage) msg);
            case PUBACK:
                return mqttPubAckHandler.preHandler(ctx, (MqttPubAckMessage) msg);
            case PINGREQ:
                return mqttPingHandler.preHandler(ctx, msg);
            case UNSUBSCRIBE:
                return mqttUnSubscribeHandler.preHandler(ctx, (MqttUnsubscribeMessage) msg);
            case PUBREL:
                return mqttPubRelHandler.preHandler(ctx, msg);
            case PUBREC:
                return mqttPubRecHandler.preHandler(ctx, msg);
            case PUBCOMP:
                return mqttPubCompHandler.preHandler(ctx, msg);
            case DISCONNECT:
                return mqttDisconnectHandler.preHandler(ctx, msg);
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
