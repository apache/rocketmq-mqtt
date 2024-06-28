package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class CoapHandler extends SimpleChannelInboundHandler<CoapMessage> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, CoapMessage coapMessage) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
