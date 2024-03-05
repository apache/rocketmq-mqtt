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

package org.apache.rocketmq.mqtt.ds.auth;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.AuthManager;
import org.apache.rocketmq.mqtt.common.hook.AbstractUpstreamHook;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookEnum;
import org.apache.rocketmq.mqtt.common.hook.UpstreamHookManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A Sample For Auth, Check sign
 */
public class AuthManagerSample extends AbstractUpstreamHook implements AuthManager {

    @Resource
    private UpstreamHookManager upstreamHookManager;

    @Resource
    private ServiceConf serviceConf;

    public Executor executor;

    public void init() {
        executor = new ThreadPoolExecutor(
                8,
                16,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryImpl("AuthHook_"));
        register();
    }

    @Override
    public void register() {
        upstreamHookManager.addHook(UpstreamHookEnum.AUTH.ordinal(), this);
    }

    @Override
    public CompletableFuture<HookResult> processMqttMessage(MqttMessageUpContext context, MqttMessage message) {
        return CompletableFuture.supplyAsync(() -> doAuth(message), executor);
    }

    @Override
    public HookResult doAuth(MqttMessage message) {
        if (message instanceof MqttConnectMessage) {
            MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) message;
            MqttConnectPayload mqttConnectPayload = mqttConnectMessage.payload();
            String clientId = mqttConnectPayload.clientIdentifier();
            String username = mqttConnectPayload.userName();
            byte[] password = mqttConnectPayload.passwordInBytes();
            boolean validateSign = false;
            try {
                validateSign = HmacSHA1Util.validateSign(clientId, password, serviceConf.getSecretKey());
            } catch (Exception e) {
                logger.error("", e);
            }
            if (!Objects.equals(username, serviceConf.getUsername()) || !validateSign) {
                if (mqttConnectMessage.variableHeader().version() == 5) {
                    return new HookResult(HookResult.FAIL, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
                }
                return new HookResult(HookResult.FAIL, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
            }
        }
        return new HookResult(HookResult.SUCCESS, null, null);
    }

}
