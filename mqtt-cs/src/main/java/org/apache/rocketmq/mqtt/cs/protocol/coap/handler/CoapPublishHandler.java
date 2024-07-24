package org.apache.rocketmq.mqtt.cs.protocol.coap.handler;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.*;
import org.apache.rocketmq.mqtt.cs.protocol.CoapPacketHandler;
import org.springframework.stereotype.Component;

@Component
public class CoapPublishHandler implements CoapPacketHandler<CoapRequestMessage> {

    @Override
    public boolean preHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        // todo: check token if connection mode
        return true;
    }

    @Override
    public void doHandler(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, HookResult upstreamHookResult) {
        if (upstreamHookResult.isSuccess()) {
            doResponseSuccess(ctx, coapMessage);
        } else {
            doResponseFail(ctx, coapMessage, upstreamHookResult.getRemark());
        }
    }

    public void doResponseFail(ChannelHandlerContext ctx, CoapRequestMessage coapMessage, String errContent) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                coapMessage.getTokenLength(),
                CoapMessageCode.INTERNAL_SERVER_ERROR,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                errContent.getBytes(),
                coapMessage.getRemoteAddress()
        );
        ctx.writeAndFlush(response);
    }

    public void doResponseSuccess(ChannelHandlerContext ctx, CoapRequestMessage coapMessage) {
        CoapMessage response = new CoapMessage(
                Constants.COAP_VERSION,
                coapMessage.getType() == CoapMessageType.CON ? CoapMessageType.ACK : CoapMessageType.NON,
                coapMessage.getTokenLength(),
                CoapMessageCode.CREATED,
                coapMessage.getMessageId(),
                coapMessage.getToken(),
                null,
                coapMessage.getRemoteAddress()
        );
        ctx.writeAndFlush(response);
    }
}
