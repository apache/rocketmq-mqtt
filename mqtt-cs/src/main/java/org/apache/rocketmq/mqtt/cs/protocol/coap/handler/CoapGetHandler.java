package org.apache.rocketmq.mqtt.cs.protocol.coap.handler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.CoapPacketHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapMessage;

public class CoapGetHandler implements CoapPacketHandler<CoapMessage> {
    @Override
    public boolean preHandler(ChannelHandlerContext ctx, CoapMessage coapMessage) {
        return false;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, CoapMessage coapMessage, HookResult upstreamHookResult) {

    }
}
