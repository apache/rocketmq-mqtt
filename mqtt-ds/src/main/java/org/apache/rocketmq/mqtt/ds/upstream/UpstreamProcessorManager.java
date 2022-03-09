/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.ds.upstream;


import io.netty.handler.codec.mqtt.MqttMessage;

import org.apache.rocketmq.mqtt.common.hook.AbstractUpstreamHook;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookEnum;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.upstream.processor.ConnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.processor.DisconnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.processor.PublishProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.processor.SubscribeProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.processor.UnSubscribeProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;

@Component
public class UpstreamProcessorManager extends AbstractUpstreamHook {

    @Resource
    private UpstreamHookManager upstreamHookManager;

    @Resource
    private ConnectProcessor connectProcessor;

    @Resource
    private PublishProcessor publishProcessor;

    @Resource
    private SubscribeProcessor subscribeProcessor;

    @Resource
    private UnSubscribeProcessor unSubscribeProcessor;

    @Resource
    private DisconnectProcessor disconnectProcessor;

    @PostConstruct
    @Override
    public void register() {
        upstreamHookManager.addHook(UpstreamHookEnum.UPSTREAM_PROCESS.ordinal(), this);
    }

    @Override
    public CompletableFuture<HookResult> processMqttMessage(MqttMessageUpContext context, MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case CONNECT:
                return connectProcessor.process(context, message);
            case PUBLISH:
                return publishProcessor.process(context, message);
            case SUBSCRIBE:
                return subscribeProcessor.process(context, message);
            case UNSUBSCRIBE:
                return unSubscribeProcessor.process(context, message);
            case DISCONNECT:
                return disconnectProcessor.process(context, message);
            default:
        }
        CompletableFuture<HookResult> hookResult = new CompletableFuture<>();
        hookResult.complete(new HookResult(HookResult.FAIL, "InvalidMqttMsgType", null));
        return hookResult;
    }

}
