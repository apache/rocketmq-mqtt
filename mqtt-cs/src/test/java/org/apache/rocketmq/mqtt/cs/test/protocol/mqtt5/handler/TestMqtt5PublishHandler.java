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

package org.apache.rocketmq.mqtt.cs.test.protocol.mqtt5.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.cs.channel.ChannelCloseFrom;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.handler.MqttPublishHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.handler.Mqtt5PublishHandler;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.PAYLOAD_FORMAT_INDICATOR;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER;
import static io.netty.handler.codec.mqtt.MqttProperties.MqttPropertyType.TOPIC_ALIAS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMqtt5PublishHandler {
    private final String topicName = "testMqttPub";
    private final int packetId = 666;
    private MqttPublishVariableHeader variableHeader = null;
    private Mqtt5PublishHandler publishHandler = new Mqtt5PublishHandler();

    private MqttFixedHeader atMostHeader;
    private MqttFixedHeader atLeastHeader;
    private MqttFixedHeader exactlyHeader;

    private MqttPublishMessage atMostPubMessage;
    private MqttPublishMessage atLeastPubMessage;
    private MqttPublishMessage exactlyPubMessage;
    private MqttPublishMessage noTopicName;

    private HookResult failHook;
    private HookResult successHook;

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private ChannelManager channelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Spy
    private NioSocketChannel channel;

    @Mock
    private ConnectConf connectConf;

    @Before
    public void setUp() throws Exception {
        FieldUtils.writeDeclaredField(publishHandler, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(publishHandler, "inFlyCache", inFlyCache, true);
        FieldUtils.writeDeclaredField(publishHandler, "channelManager", channelManager, true);

        MqttProperties props = new MqttProperties();
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 10));
        props.add(new MqttProperties.IntegerProperty(SUBSCRIPTION_IDENTIFIER.value(), 20));
        props.add(new MqttProperties.IntegerProperty(PAYLOAD_FORMAT_INDICATOR.value(), 6));
        props.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), 10));
        props.add(new MqttProperties.UserProperty("isSecret", "true"));
        props.add(new MqttProperties.UserProperty("tag", "firstTag"));
        props.add(new MqttProperties.UserProperty("tag", "secondTag"));

        variableHeader = new MqttPublishVariableHeader(topicName, packetId, props);

        atMostHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0);
        atMostPubMessage = new MqttPublishMessage(atMostHeader, variableHeader, null);
        atLeastHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        atLeastPubMessage = new MqttPublishMessage(atLeastHeader, variableHeader, null);
        exactlyHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.EXACTLY_ONCE, false, 0);
        exactlyPubMessage = new MqttPublishMessage(exactlyHeader, variableHeader, null);

        MqttProperties propsNoTopicName = new MqttProperties();
        propsNoTopicName.add(new MqttProperties.IntegerProperty(TOPIC_ALIAS.value(), 10));
        MqttPublishVariableHeader variableHeaderNoTopicName = new MqttPublishVariableHeader("", packetId, propsNoTopicName);
        noTopicName = new MqttPublishMessage(atLeastHeader, variableHeaderNoTopicName, Unpooled.buffer());

        failHook = new HookResult(HookResult.FAIL, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED.byteValue(),
                Remark.INVALID_PARAM, null);
        successHook = new HookResult(HookResult.SUCCESS, Remark.SUCCESS, null);

        when(ctx.channel()).thenReturn(channel);
        doReturn(null).when(channel).writeAndFlush(any());
    }

    @Test
    public void testDoHandlerHookFail() {
        publishHandler.doHandler(ctx, atMostPubMessage, failHook);

        verify(ctx).channel();
        verify(channelManager).closeConnect(eq(channel), eq(ChannelCloseFrom.SERVER), eq(Remark.INVALID_PARAM));
        verifyNoMoreInteractions(inFlyCache, channelManager, ctx);
    }

    @Test
    public void testDoHandlerAtMostOnce() {
        publishHandler.doHandler(ctx, atMostPubMessage, successHook);

        verify(ctx).channel();
        verifyNoMoreInteractions(inFlyCache, channelManager, ctx);
    }

    @Test
    public void testDoHandlerAtLeastOnce() {
        publishHandler.doHandler(ctx, atLeastPubMessage, successHook);

        verify(ctx, times(2)).channel();
        verify(channel).writeAndFlush(any(MqttPubAckMessage.class));
        verifyNoMoreInteractions(inFlyCache, channelManager, ctx);
    }

    @Test
    public void testDoHandlerExactlyOnceCacheHit() {
        doReturn(true).when(inFlyCache).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));

        publishHandler.doHandler(ctx, exactlyPubMessage, successHook);

        verify(ctx, times(2)).channel();
        verify(inFlyCache).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));
        verify(channel).writeAndFlush(any(MqttMessage.class));
        verifyNoMoreInteractions(inFlyCache, channelManager, ctx);
    }

    @Test
    public void testDoHandlerExactlyOnceCacheNotHit() {
        doReturn(false).when(inFlyCache).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));

        publishHandler.doHandler(ctx, exactlyPubMessage, successHook);

        verify(ctx, times(2)).channel();
        verify(inFlyCache).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));
        verify(channel).writeAndFlush(any(MqttMessage.class));
        verify(inFlyCache).put(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));
        verifyNoMoreInteractions(inFlyCache, channelManager, ctx);
    }

    @Test
    public void testPreHandlerTopicAlias() {
        doReturn(false).when(inFlyCache).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));
        when(connectConf.getTopicAliasMaximum()).thenReturn(1000);

        publishHandler.preHandler(ctx, exactlyPubMessage);

        Assert.assertTrue(publishHandler.preHandler(ctx, noTopicName));

        verify(ctx, times(2)).channel();
        verify(inFlyCache, times(2)).contains(eq(InFlyCache.CacheType.PUB), anyString(), eq(packetId));
        verifyNoMoreInteractions(inFlyCache, ctx);
    }
}