package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class CoAPPacketDispatcher extends SimpleChannelInboundHandler<CoAPMessage> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, CoAPMessage coAPMessage) throws Exception {

    }
}
