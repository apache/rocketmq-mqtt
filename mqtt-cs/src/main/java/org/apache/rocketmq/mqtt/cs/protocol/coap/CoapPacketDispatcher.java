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
package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapPublishHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapConnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapHeartbeatHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapDisconnectHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapAckHandler;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapPublishProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapSubscribeProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapConnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapHeartbeatProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapDisconnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.coap.processor.CoapAckProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

@Component
public class CoapPacketDispatcher extends SimpleChannelInboundHandler<CoapRequestMessage> {

    private static Logger logger = LoggerFactory.getLogger(CoapPacketDispatcher.class);
    @Resource
    private CoapPublishHandler coapPublishHandler;

    @Resource
    private CoapSubscribeHandler coapSubscribeHandler;

    @Resource
    private CoapConnectHandler coapConnectHandler;

    @Resource
    private CoapHeartbeatHandler coapHeartbeatHandler;

    @Resource
    private CoapDisconnectHandler coapDisconnectHandler;

    @Resource
    private CoapAckHandler coapAckHandler;

    @Resource
    private CoapPublishProcessor coapPublishProcessor;

    @Resource
    private CoapSubscribeProcessor coapSubscribeProcessor;

    @Resource
    private CoapConnectProcessor coapConnectProcessor;

    @Resource
    private CoapHeartbeatProcessor coapHeartbeatProcessor;

    @Resource
    private CoapDisconnectProcessor coapDisconnectProcessor;

    @Resource
    private CoapAckProcessor coapAckProcessor;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CoapRequestMessage msg) throws Exception {

        boolean preResult = preHandler(ctx, msg);
        if (!preResult) {
            return;
        }
        CompletableFuture<HookResult> processResult;
        try {
            processResult = processCoapMessage(msg);
            if (processResult == null) {
                _channelRead0(ctx, msg, null);
                return;
            }
        } catch (Throwable t) {
            logger.error("", t);
            throw new ChannelException(t.getMessage());
        }
        processResult.whenComplete((hookResult, throwable) -> {
            if (throwable != null) {
                logger.error("", throwable);
                ctx.fireExceptionCaught(new ChannelException(throwable.getMessage()));
                return;
            }
            if (hookResult == null) {
                ctx.fireExceptionCaught(new ChannelException("Coap UpstreamHook Result Unknown"));
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

    private  void _channelRead0(ChannelHandlerContext ctx, CoapRequestMessage msg, HookResult processResult) {
        switch (msg.getRequestType()) {
            case PUBLISH:
                coapPublishHandler.doHandler(ctx, msg, processResult);
                break;
            case SUBSCRIBE:
                coapSubscribeHandler.doHandler(ctx, msg, processResult);
                break;
            case CONNECT:
                coapConnectHandler.doHandler(ctx, msg, processResult);
                break;
            case HEARTBEAT:
                coapHeartbeatHandler.doHandler(ctx, msg, processResult);
                break;
            case DISCONNECT:
                coapDisconnectHandler.doHandler(ctx, msg, processResult);
                break;
            case ACK:
                coapAckHandler.doHandler(ctx, msg, processResult);
                break;
            default:
        }
    }

    private boolean preHandler(ChannelHandlerContext ctx, CoapRequestMessage msg) {
        switch (msg.getRequestType()) {
            case PUBLISH:
                return coapPublishHandler.preHandler(ctx, msg);
            case SUBSCRIBE:
                return coapSubscribeHandler.preHandler(ctx, msg);
            case CONNECT:
                return coapConnectHandler.preHandler(ctx, msg);
            case HEARTBEAT:
                return coapHeartbeatHandler.preHandler(ctx, msg);
            case DISCONNECT:
                return coapDisconnectHandler.preHandler(ctx, msg);
            case ACK:
                return coapAckHandler.preHandler(ctx, msg);
            default:
                return false;
        }
    }

    public CompletableFuture<HookResult> processCoapMessage(CoapRequestMessage msg) {
        switch (msg.getRequestType()) {
            case PUBLISH:
                return coapPublishProcessor.process(msg);
            case SUBSCRIBE:
                return coapSubscribeProcessor.process(msg);
            case CONNECT:
                return coapConnectProcessor.process(msg);
            case HEARTBEAT:
                return coapHeartbeatProcessor.process(msg);
            case DISCONNECT:
                return coapDisconnectProcessor.process(msg);
            case ACK:
                return coapAckProcessor.process(msg);
            default:
        }
        CompletableFuture<HookResult> hookResult = new CompletableFuture<>();
        hookResult.complete(new HookResult(HookResult.FAIL, "InvalidCoapMsgCode", null));
        return hookResult;
    }

}
