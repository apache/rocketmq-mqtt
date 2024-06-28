package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapDeleteHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapGetHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapPostHandler;
import org.apache.rocketmq.mqtt.cs.protocol.coap.handler.CoapPutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

public class CoapPacketDispatcher extends SimpleChannelInboundHandler<CoapMessage> {
    private static Logger logger = LoggerFactory.getLogger(CoapPacketDispatcher.class);

    @Resource
    private CoapGetHandler coapGetHandler;

    @Resource
    private CoapPostHandler coapPostHandler;

    @Resource
    private CoapPutHandler coapPutHandler;

    @Resource
    private CoapDeleteHandler coapDeleteHandler;

//    @Resource
//    private UpstreamHookManager upstreamHookManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CoapMessage msg) throws Exception {

        boolean preResult = preHandler(ctx, msg);
        if (!preResult) {
            return;
        }

        CompletableFuture<HookResult> upstreamHookResult;
        try {
//            upstreamHookResult = upstreamHookManager.doUpstreamHook(ctx, msg);
            upstreamHookResult = null;
            if (upstreamHookResult == null) {
                _channelRead0(ctx, msg, null);
                return;
            }
        } catch (Throwable t) {
            logger.error("", t);
            throw new ChannelException(t.getMessage());
        }

        upstreamHookResult.whenComplete((coapHookResult, throwable) -> {
            if (throwable != null) {
                // TODO
                return;
            }
            if (coapHookResult == null) {
                // TODO
                return;
            }
            try {
                _channelRead0(ctx, msg, coapHookResult);
            } catch (Throwable t) {
                logger.error("", t);
                // TODO
            }

        });
    }

    private  void _channelRead0(ChannelHandlerContext ctx, CoapMessage msg, HookResult coapUpstreamHookResult) {

    }

    private boolean preHandler(ChannelHandlerContext ctx, CoapMessage msg) {
        return true;
    }

    // Type: CON/NON/ACK/RST
    // Code: GET/POST/PUT/DELETE
    // Handle Options
    // Handle Payload

}
