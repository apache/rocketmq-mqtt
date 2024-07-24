package org.apache.rocketmq.mqtt.ds.upstream.coap.processor;

import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.ds.upstream.coap.CoapUpstreamProcessor;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class CoapConnectProcessor implements CoapUpstreamProcessor {
    @Override
    public CompletableFuture<HookResult> process(CoapRequestMessage msg) {
        // todo: RPC broadcast (store token)
        return null;
    }
}
