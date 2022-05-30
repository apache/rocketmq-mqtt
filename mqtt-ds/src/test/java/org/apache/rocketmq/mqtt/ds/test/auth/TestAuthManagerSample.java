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

package org.apache.rocketmq.mqtt.ds.test.auth;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.apache.rocketmq.mqtt.ds.auth.AuthManagerSample;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestAuthManagerSample {

    @Mock
    private ServiceConf serviceConf;

    @Test
    public void testAuth() throws NoSuchAlgorithmException, InvalidKeyException, IllegalAccessException {
        String username = "test";
        String secretKey = "test";
        when(serviceConf.getUsername()).thenReturn(username);
        when(serviceConf.getSecretKey()).thenReturn(secretKey);
        AuthManagerSample authManagerSample = new AuthManagerSample();
        FieldUtils.writeDeclaredField(authManagerSample, "serviceConf", serviceConf, true);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT,
                false,
                MqttQoS.AT_LEAST_ONCE,
                false, 1
        );
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                null,
                4,
                true,
                true,
                false,
                1,
                false,
                true,
                60
        );
        String clientId = "test";
        byte[] password = HmacSHA1Util.macSignature(clientId, secretKey).getBytes(StandardCharsets.UTF_8);
        MqttConnectPayload payload = new MqttConnectPayload(clientId, null, null, username, password);
        MqttConnectMessage message = new MqttConnectMessage(mqttFixedHeader, variableHeader, payload);
        HookResult result = authManagerSample.doAuth(message);
        Assert.assertTrue(result.isSuccess());
    }
}
