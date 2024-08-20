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

import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.ds.auth.CoapAuthManager;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertFalse;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapAuthManager {
    private static final String username = "user111";
    private static final String secretKey = "password111";

    private CoapAuthManager coapAuthManager;

    @Mock
    private ServiceConf serviceConf;

    @Before
    public void setUp() throws Exception {
        coapAuthManager = new CoapAuthManager();
        FieldUtils.writeDeclaredField(coapAuthManager, "serviceConf", serviceConf, true);

        when(serviceConf.getUsername()).thenReturn(username);
        when(serviceConf.getSecretKey()).thenReturn(secretKey);

        coapAuthManager.init();
    }

    @Test
    public void doAuthWrongUsername() throws ExecutionException, InterruptedException {
        String wrongUsername = "user222";
        CompletableFuture<HookResult> future = coapAuthManager.doAuth(wrongUsername, secretKey);
        assertFalse(future.get().isSuccess());
    }

    @Test
    public void doAuthWrongPassword() throws ExecutionException, InterruptedException {
        String wrongPassword = "password222";
        CompletableFuture<HookResult> future = coapAuthManager.doAuth(username, wrongPassword);
        assertFalse(future.get().isSuccess());
    }

    @Test
    public void doAuthSuccess() throws ExecutionException, InterruptedException {
        CompletableFuture<HookResult> future = coapAuthManager.doAuth(username, secretKey);
        assertTrue(future.get().isSuccess());
    }

}
