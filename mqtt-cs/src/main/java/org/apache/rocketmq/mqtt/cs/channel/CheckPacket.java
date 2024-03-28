package org.apache.rocketmq.mqtt.cs.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@ChannelHandler.Sharable
@Component
public class CheckPacket extends ChannelOutboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(CheckPacket.class);

    @Resource
    private ConnectConf connectConf;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        Channel channel = ctx.channel();
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            int size = buf.readableBytes();
            if (size > connectConf.getMaxPacketSizeInByte()) {
                log.error("packet size too large, channel: {}, size: {}", channel, size);
                throw new RuntimeException("packet size too large");
            }

        }

        ctx.write(msg, promise);
    }
}
