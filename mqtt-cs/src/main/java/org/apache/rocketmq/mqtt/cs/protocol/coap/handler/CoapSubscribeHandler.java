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

package org.apache.rocketmq.mqtt.cs.protocol.coap.handler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.*;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.protocol.CoapPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttSubscribeHandler;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.loop.CoapSessionLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class CoapSubscribeHandler implements CoapPacketHandler<CoapRequestMessage> {

    private static Logger logger = LoggerFactory.getLogger(CoapSubscribeHandler.class);

    @Resource
    private CoapSessionLoop sessionLoop;

    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("check_coap_subscribe_future"));

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        // todo: check token if connection mode
//        byte[] byteArray = new byte[3];
//        intToByteArray(1, byteArray);
//        doResponseSuccess(ctx, coapMessage, byteArray);
//
//        coapMessage.setMessageId(coapMessage.getMessageId() + 1);
//        intToByteArray(2, byteArray);
//        doResponseCON(ctx, coapMessage, byteArray);
//
//        coapMessage.setMessageId(coapMessage.getMessageId() + 1);
//        intToByteArray(3, byteArray);
//        doResponseCON(ctx, coapMessage, byteArray);
//
//        coapMessage.setMessageId(coapMessage.getMessageId() + 1);
//        intToByteArray(4, byteArray);
//        doResponseCON(ctx, coapMessage, byteArray);
//
//        coapMessage.setMessageId(coapMessage.getMessageId() + 1);
//        intToByteArray(5, byteArray);
//        doResponseCONError(ctx, coapMessage, byteArray);
//
//        coapMessage.setMessageId(coapMessage.getMessageId() + 1);
//        intToByteArray(6, byteArray);
//        doResponseCON(ctx, coapMessage, byteArray);
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, HookResult upstreamHookResult) {
        // todo: response ack
        if (!upstreamHookResult.isSuccess()) {
            doResponseFail(ctx, coapMessage, upstreamHookResult.getRemark());
            return;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        // todo: setFuture
        scheduler.schedule(() -> {
            if (!future.isDone()) {
                future.complete(null);
            }
        }, 1, TimeUnit.SECONDS);
        try {
            Subscription subscription = new Subscription();
            subscription.setQos(coapMessage.getQosLevel().value());
            subscription.setTopicFilter(TopicUtils.normalizeTopic(coapMessage.getTopic()));

            CoapSession session = new CoapSession();
            session.setAddress(coapMessage.getRemoteAddress());
            session.setMessageId(coapMessage.getMessageId());
            session.setToken(coapMessage.getToken());
            session.setSubscribeTime(System.currentTimeMillis());
            session.setSubscription(subscription);
            boolean addResult = sessionLoop.addSession(session); // if the session is already exist, do not send retained message

            future.thenAccept(aVoid -> {
                if (!ctx.channel().isActive()) {
                    return;
                }
                // todo: removeFuture
                doResponseSuccess(ctx, coapMessage);
                // todo: sendRetainMessage()


            });
        } catch (Exception e) {
            logger.error("Coap Subscribe:{}", coapMessage.getRemoteAddress(), e);
        }

    }

    public void doResponseFail(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, String errContent) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.ACK,
                coapMessage.getTokenLength(),
                CoapMessageCode.INTERNAL_SERVER_ERROR,
                coapMessage.getMessageId() + 1,
                coapMessage.getToken(),
                errContent.getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, intToByteArray(1)));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }
    }


    // change an integer into a byte array with length 3
    private byte[] intToByteArray(int value) {
        byte[] byteArray = new byte[3];
        byteArray[0] = (byte) (value >> 16);
        byteArray[1] = (byte) (value >> 8);
        byteArray[2] = (byte) (value);
        return byteArray;
    }

    public void doResponseSuccess(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                coapMessage.getTokenLength(),
                CoapMessageCode.CONTENT,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                "Hello, I have accept your request successfully!".getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, intToByteArray(1)));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }

    }

    public void doResponseNON(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, byte[] byteArray) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.NON,
                coapMessage.getTokenLength(),
                CoapMessageCode.CONTENT,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                "Hello, 111".getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, byteArray));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }
    }

    public void doResponseCON(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, byte[] byteArray) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.CON,
                coapMessage.getTokenLength(),
                CoapMessageCode.CONTENT,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                "Hello, 222".getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, byteArray));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }
    }

    public void doResponseCONError(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, byte[] byteArray) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.CON,
                coapMessage.getTokenLength(),
                CoapMessageCode.BAD_REQUEST,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                "Hello, 222".getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, byteArray));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }
    }


    public void doResponseRST(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, byte[] byteArray) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                CoapMessageType.RST,
                coapMessage.getTokenLength(),
                CoapMessageCode.BAD_REQUEST,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                "Hello, RST".getBytes(StandardCharsets.UTF_8),
                coapMessage.getRemoteAddress()
        );
        response.addOption(new CoapMessageOption(CoapMessageOptionNumber.OBSERVE, byteArray));
        if (ctx.channel().isActive()) {
            ctx.writeAndFlush(response);
        } else {
            System.out.println("Channel is not active");
        }
    }
}
