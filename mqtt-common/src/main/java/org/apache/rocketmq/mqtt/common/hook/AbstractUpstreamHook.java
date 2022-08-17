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

package org.apache.rocketmq.mqtt.common.hook;

import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;


public abstract class AbstractUpstreamHook implements UpstreamHook {
    public static Logger logger = LoggerFactory.getLogger(AbstractUpstreamHook.class);
    public UpstreamHook nextUpstreamHook;

    @Override
    public void setNextHook(Hook hook) {
        this.nextUpstreamHook = (UpstreamHook) hook;
    }

    @Override
    public Hook getNextHook() {
        return this.nextUpstreamHook;
    }

    @Override
    public CompletableFuture<HookResult> doHook(MqttMessageUpContext context, MqttMessage msg) {
        try {
            CompletableFuture<HookResult> result = processMqttMessage(context,msg);
            if (nextUpstreamHook == null) {
                return result;
            }
            return result.thenCompose(hookResult -> {
                if (!hookResult.isSuccess()) {
                    CompletableFuture<HookResult> nextHookResult = new CompletableFuture<>();
                    nextHookResult.complete(hookResult);
                    return nextHookResult;
                }
                return nextUpstreamHook.doHook(context, msg);
            });
        } catch (Throwable t) {
            logger.error("",t);
            CompletableFuture<HookResult> result = new CompletableFuture<>();
            result.completeExceptionally(t);
            return result;
        }
    }

    public abstract void register();

    public abstract CompletableFuture<HookResult> processMqttMessage(MqttMessageUpContext context, MqttMessage message) ;

}
