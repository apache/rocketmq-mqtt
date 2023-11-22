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

package org.apache.rocketmq.mqtt.ds.upstream.mqtt5;


import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.mqtt.common.hook.AbstractUpstreamHook;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookEnum;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.ConnectProcessor5;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.DisconnectProcessor5;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.PublishProcessor5;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.SubscribeProcessor5;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt5.processor.UnSubscribeProcessor5;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.CompletableFuture;


@Component
public class UpstreamProcessorManager5 extends AbstractUpstreamHook {

    @Resource
    private UpstreamHookManager upstreamHookManager;

    @Resource
    private ConnectProcessor5 connectProcessor5;

    @Resource
    private PublishProcessor5 publishProcessor5;

    @Resource
    private SubscribeProcessor5 subscribeProcessor5;

    @Resource
    private UnSubscribeProcessor5 unSubscribeProcessor5;

    @Resource
    private DisconnectProcessor5 disconnectProcessor5;

    @PostConstruct
    @Override
    public void register() {
        upstreamHookManager.addHook(UpstreamHookEnum.UPSTREAM_PROCESS5.ordinal(), this);
    }

    @Override
    public CompletableFuture<HookResult> processMqttMessage(MqttMessageUpContext context, MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case CONNECT:
                return connectProcessor5.process(context, message);
            case PUBLISH:
                return publishProcessor5.process(context, message);
            case SUBSCRIBE:
                return subscribeProcessor5.process(context, message);
            case UNSUBSCRIBE:
                return unSubscribeProcessor5.process(context, message);
            case DISCONNECT:
                return disconnectProcessor5.process(context, message);
            default:
        }
        CompletableFuture<HookResult> hookResult = new CompletableFuture<>();
        hookResult.complete(new HookResult(HookResult.FAIL, "InvalidMqttMsgType", null));
        return hookResult;
    }

}
