/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.cs.test.session.infly;

import io.netty.channel.ChannelFuture;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.infly.InFlyCache;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.apache.rocketmq.mqtt.cs.session.infly.PushAction;
import org.apache.rocketmq.mqtt.cs.session.infly.RetryDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPushAction {
    @Mock
    private MqttMsgId mqttMsgId;

    @Mock
    private RetryDriver retryDriver;

    @Mock
    private InFlyCache inFlyCache;

    @Mock
    private ConnectConf connectConf;

    @Mock
    private Session session;

    @Mock
    private Queue queue;

    @Mock
    private Message message;

    @Spy
    private NioSocketChannel channel;

    @Spy
    private ChannelFuture channelFuture;

    private PushAction pushAction;
    private Subscription subscription;

    final String p2pTopic = "/p2p/test";
    final String channelId = "testPushAction";
    final int qos = 1;
    final int mqttId = 666;
    final long offset = 3;

    @Before
    public void before() throws IllegalAccessException {
        subscription = new Subscription(p2pTopic, qos);
        pushAction = new PushAction();
        FieldUtils.writeDeclaredField(pushAction, "mqttMsgId", mqttMsgId, true);
        FieldUtils.writeDeclaredField(pushAction, "retryDriver", retryDriver, true);
        FieldUtils.writeDeclaredField(pushAction, "inFlyCache", inFlyCache, true);
        FieldUtils.writeDeclaredField(pushAction, "connectConf", connectConf, true);

        when(session.getChannel()).thenReturn(channel);
        when(session.getChannelId()).thenReturn(channelId);
        when(session.getClientId()).thenReturn(channelId);
        when(message.getOffset()).thenReturn(offset);
        when(message.getPayload()).thenReturn(MessageUtil.EMPTYSTRING.getBytes());
    }

    @Test
    public void testMessageArriveNotOrder() {
        List<Message> messages = new ArrayList<>(Arrays.asList(message));
        when(session.pendMessageList(any(), any())).thenReturn(messages);
        when(connectConf.isOrder()).thenReturn(false);
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).push(any(), any(), any(), any());

        spyPushAction.messageArrive(session, subscription, queue);
        verify(connectConf).isOrder();
        verify(session).pendMessageList(any(), any());
        verify(spyPushAction, times(messages.size())).push(any(), any(), any(), any());
        verifyNoMoreInteractions(session, connectConf);
    }

    @Test
    public void testMessageArriveByOrder() {
        when(connectConf.isOrder()).thenReturn(true);
        when(retryDriver.needRetryBefore(any(), any(), any())).thenReturn(false);
        when(session.nextSendMessageByOrder(any(), any())).thenReturn(message);
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).push(any(), any(), any(), any());

        spyPushAction.messageArrive(session, subscription, queue);
        verify(connectConf).isOrder();
        verify(retryDriver).needRetryBefore(any(), any(), any());
        verify(session).nextSendMessageByOrder(any(), any());
        verify(spyPushAction).push(any(), any(), any(), any());
        verifyNoMoreInteractions(session, connectConf);
    }

    @Test
    public void testPushWhenNotClean() {
        when(session.isClean()).thenReturn(false);
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());

        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
    }

    @Test
    public void testPushWhenIsClean() {
        when(session.isClean()).thenReturn(true);
        long msgStoreTime = 1000;
        long sessionStartTime = 2000;
        when(message.getStoreTimestamp()).thenReturn(msgStoreTime);
        when(session.getStartTime()).thenReturn(sessionStartTime);

        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).rollNext(any(), anyInt());
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());

        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).rollNext(any(), anyInt());
        verify(spyPushAction, times(0)).write(any(), any(), anyInt(), anyInt(), any());
    }

    @Test
    public void testPushWhenQosZero() {
        subscription.setQos(0);
        when(session.isClean()).thenReturn(false);
        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
        doNothing().when(spyPushAction).rollNextByAck(any(), anyInt());
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());

        if (message.getPayload() == null) {
            message.setPayload(new byte[10]);
            message.setPayload(MessageUtil.EMPTYSTRING.getBytes());
        }
        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
        verify(spyPushAction).rollNextByAck(any(), anyInt());
    }

    @Test
    public void testPushQos() {
        subscription.setTopicFilter("/test/qos");
        when(session.isClean()).thenReturn(false);
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());

        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).write(any(), any(), anyInt(), anyInt(), any());
        doNothing().when(spyPushAction).rollNextByAck(any(), anyInt());

        when(message.qos()).thenReturn(1);
        subscription.setQos(1);
        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).write(any(), any(), anyInt(), eq(1), any());

        clearInvocations(spyPushAction);
        when(message.qos()).thenReturn(1);
        subscription.setQos(2);
        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).write(any(), any(), anyInt(), eq(1), any());

        clearInvocations(spyPushAction);
        when(message.qos()).thenReturn(3);
        subscription.setQos(2);
        spyPushAction.push(message, subscription, session, queue);
        verify(spyPushAction).write(any(), any(), anyInt(), eq(2), any());
    }

    @Test
    public void testWrite() {
        doReturn(channelFuture).when(channel).writeAndFlush(any());
        when(message.getOriginTopic()).thenReturn(p2pTopic);
        when(message.getPayload()).thenReturn("test".getBytes(StandardCharsets.UTF_8));

        pushAction.write(session, message, mqttId, qos, subscription);
        verify(channel).writeAndFlush(any());
        verify(channelFuture).addListener(any());
    }

    @Test
    public void testRollNextByAck() {
        doNothing().when(mqttMsgId).releaseId(anyInt(), anyString());
        InFlyCache.PendingDownCache cache = new InFlyCache().getPendingDownCache();
        cache.put(channelId, mqttId, subscription, queue, message);
        when(inFlyCache.getPendingDownCache()).thenReturn(cache);
        when(connectConf.isOrder()).thenReturn(true);
        when(session.rollNext(subscription, queue, offset)).thenReturn(message);

        PushAction spyPushAction = spy(pushAction);
        doNothing().when(spyPushAction).push(any(), any(), any(), any());

        spyPushAction.rollNextByAck(session, mqttId);
        verify(spyPushAction).rollNextByAck(any(), anyInt());
        verify(spyPushAction).rollNext(any(), anyInt());
        verify(spyPushAction)._rollNext(any(), any());
        verify(spyPushAction).push(any(), any(), any(), any());
        verify(mqttMsgId, times(2)).releaseId(anyInt(), anyString());
        verify(inFlyCache, times(2)).getPendingDownCache();
        verifyNoMoreInteractions(spyPushAction, mqttMsgId, inFlyCache);
    }

    @Test
    public void testRollNexNoWaitRetry() {
        when(inFlyCache.getPendingDownCache()).thenReturn(new InFlyCache().getPendingDownCache());
        PushAction spyPushAction = spy(pushAction);
        spyPushAction.rollNextNoWaitRetry(session, 666);
        verify(spyPushAction).rollNextNoWaitRetry(any(), anyInt());
        verifyNoMoreInteractions(spyPushAction);
    }

    @Test
    public void testRollNextIsNotOrder() {
        InFlyCache.PendingDown pendingDown = new InFlyCache().getPendingDownCache().put(
            channelId, mqttId, subscription, queue, message);
        when(connectConf.isOrder()).thenReturn(false);

        pushAction._rollNext(session, pendingDown);
        verify(session).ack(eq(subscription), eq(queue), eq(offset));
        verifyNoMoreInteractions(session);
    }

}
