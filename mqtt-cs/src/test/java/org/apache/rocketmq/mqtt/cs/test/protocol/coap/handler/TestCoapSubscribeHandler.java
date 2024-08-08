package org.apache.rocketmq.mqtt.cs.test.protocol.coap.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapMessageCode;
import org.apache.rocketmq.mqtt.common.model.CoapMessageType;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.cs.channel.DatagramChannelManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapSubscribeHandler;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.loop.CoapSessionLoop;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapSubscribeHandler {

    private CoapSubscribeHandler coapSubscribeHandler;

    @Mock
    private CoapSessionLoop sessionLoop;

    @Mock
    private RetainedPersistManager retainedPersistManager;

    @Mock
    private DatagramChannelManager datagramChannelManager;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private CoapRequestMessage coapMessage;

    @Mock
    private CoapSession session;

    @Mock
    private Channel channel;

    @Before
    public void setUp() throws IllegalAccessException {
        coapSubscribeHandler = new CoapSubscribeHandler();
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "sessionLoop", sessionLoop, true);
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "retainedPersistManager", retainedPersistManager, true);
        FieldUtils.writeDeclaredField(coapSubscribeHandler, "datagramChannelManager", datagramChannelManager, true);
    }

    @Test
    public void testPreHandler() {
        boolean result = coapSubscribeHandler.preHandler(ctx, coapMessage);
        assertTrue(result);
    }

    @Test
    public void testDoHandlerUpstreamFail() {
        HookResult failHookResult = new HookResult(HookResult.FAIL, "Error", null);
        when(coapMessage.getTokenLength()).thenReturn(0);
        when(coapMessage.getMessageId()).thenReturn(1);
        when(coapMessage.getToken()).thenReturn(null);
        when(coapMessage.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9675));

        coapSubscribeHandler.doHandler(ctx, coapMessage, failHookResult);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.INTERNAL_SERVER_ERROR);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoHanlderOldSession() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        when(coapMessage.getQosLevel()).thenReturn(MqttQoS.AT_LEAST_ONCE);
        when(coapMessage.getTopic()).thenReturn("topic1/r1");
        when(coapMessage.getType()).thenReturn(CoapMessageType.CON);
        when(coapMessage.getTokenLength()).thenReturn(0);
        when(coapMessage.getMessageId()).thenReturn(1);
        when(coapMessage.getToken()).thenReturn(null);
        when(coapMessage.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9675));
        when(sessionLoop.getSession(any())).thenReturn(session);

        coapSubscribeHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(sessionLoop).getSession(any());
        verify(session).refreshSubscribeTime();
        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.CONTENT);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoHanlderNewSession() {
        HookResult successHookResult = new HookResult(HookResult.SUCCESS, null, null);
        when(coapMessage.getQosLevel()).thenReturn(MqttQoS.AT_LEAST_ONCE);
        when(coapMessage.getTopic()).thenReturn("topic1/r1");
        when(coapMessage.getToken()).thenReturn(null);
        when(coapMessage.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9675));
        when(sessionLoop.getSession(any())).thenReturn(null);

        coapSubscribeHandler.doHandler(ctx, coapMessage, successHookResult);

        verify(sessionLoop).getSession(any());
        verify(sessionLoop).addSession(any(), any());
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoResponseFail() {
        when(coapMessage.getTokenLength()).thenReturn(0);
        when(coapMessage.getMessageId()).thenReturn(1);
        when(coapMessage.getToken()).thenReturn(null);
        when(coapMessage.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9675));

        coapSubscribeHandler.doResponseFail(coapMessage, "Error");

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.INTERNAL_SERVER_ERROR);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }

    @Test
    public void testDoResponseSuccess() {
        when(coapMessage.getType()).thenReturn(CoapMessageType.CON);
        when(coapMessage.getTokenLength()).thenReturn(0);
        when(coapMessage.getMessageId()).thenReturn(1);
        when(coapMessage.getToken()).thenReturn(null);
        when(coapMessage.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 9675));
        when(session.getMessageNum()).thenReturn(1);

        coapSubscribeHandler.doResponseSuccess(coapMessage, session);

        verify(datagramChannelManager).writeResponse(argThat(response -> {
            assertEquals(response.getCode(), CoapMessageCode.CONTENT);
            return true;
        }));
        verifyNoMoreInteractions(ctx, sessionLoop, retainedPersistManager, datagramChannelManager);
    }
}
