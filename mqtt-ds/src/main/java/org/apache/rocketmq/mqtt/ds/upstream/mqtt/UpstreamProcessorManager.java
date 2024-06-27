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

package org.apache.rocketmq.mqtt.ds.upstream.mqtt;


import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.rocketmq.mqtt.common.hook.AbstractUpstreamHook;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookEnum;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.ConnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.DisconnectProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.PublishProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.SubscribeProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.UnSubscribeProcessor;
import org.apache.rocketmq.mqtt.exporter.collector.MqttMetricsCollector;
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
        CompletableFuture<HookResult> hookResult = new CompletableFuture<>();
        if (context.getMqttVersion() != null && MqttVersion.MQTT_5.equals(context.getMqttVersion())) {
            hookResult.complete(new HookResult(HookResult.SUCCESS, null, null));
            return hookResult;
        }

        collectProcessRequestTps(message.fixedHeader().messageType().name());
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

        hookResult.complete(new HookResult(HookResult.FAIL, "InvalidMqttMsgType", null));
        return hookResult;
    }

    private void collectProcessRequestTps(String name) {
        try {
            MqttMetricsCollector.collectProcessRequestTps(1, name);
        } catch (Throwable e) {
            logger.error("Collect prometheus error", e);
        }
    }
}
