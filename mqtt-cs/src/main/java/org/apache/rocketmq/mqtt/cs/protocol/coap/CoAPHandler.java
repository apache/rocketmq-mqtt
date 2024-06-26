package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class CoAPHandler extends SimpleChannelInboundHandler<CoAPMessage> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, CoAPMessage coAPMessage) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
