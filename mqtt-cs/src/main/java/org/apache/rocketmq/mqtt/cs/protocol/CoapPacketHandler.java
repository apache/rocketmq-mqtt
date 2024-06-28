package org.apache.rocketmq.mqtt.cs.protocol;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.cs.protocol.coap.CoapMessage;

public interface CoapPacketHandler<T extends CoapMessage>{

    /**
     * preHandler before upstream processor, for preprocessing
     * @param ctx
     * @param coapMessage
     * @return
     */
    boolean preHandler(ChannelHandlerContext ctx, T coapMessage);

    /**
     * doHandler after upstream processor
     *
     * @param ctx
     * @param coapMessage
     * @param upstreamHookResult
     */
    void doHandler(ChannelHandlerContext ctx, T coapMessage, HookResult upstreamHookResult);
}
