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

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Remark;
import org.apache.rocketmq.mqtt.common.util.PasswordHashUtil;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

@Component
public class CoapAuthManager {
    public static Logger logger = LoggerFactory.getLogger(CoapAuthManager.class);

    private static String salt;
    private static final String ALGORITHM = "SHA-256";
    private static final String SALT_POSITION = "suffix";
    private static String hashedPassword;

    @Resource
    private ServiceConf serviceConf;

    @PostConstruct
    public void init() throws NoSuchAlgorithmException {
        salt = PasswordHashUtil.generateSalt(16);
        hashedPassword = PasswordHashUtil.hashWithSalt(serviceConf.getSecretKey(), salt, ALGORITHM, SALT_POSITION);
    }

    public CompletableFuture<HookResult> doAuth(String username, String password) {
        try {
            if (serviceConf.getUsername().equals(username) && PasswordHashUtil.validatePassword(password, hashedPassword, salt, ALGORITHM, SALT_POSITION)) {
                return HookResult.newHookResult(HookResult.SUCCESS, null, null);
            }
        } catch (NoSuchAlgorithmException e) {
            logger.error("", e);
        }
        return HookResult.newHookResult(HookResult.FAIL, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD.byteValue(), Remark.AUTH_FAILED, null);
    }

}
