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
package org.apache.rocketmq.mqtt.cs.test.session;

import org.apache.rocketmq.mqtt.cs.session.CoapTokenManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(MockitoJUnitRunner.class)
public class TestCoapTokenManager {

    private CoapTokenManager coapTokenManager;

    @Before
    public void setUp() {
        coapTokenManager = new CoapTokenManager();
    }

    @Test
    public void testCreateToken() {
        String clientId = "test111";
        String token = coapTokenManager.createToken(clientId);

        assertEquals(1, coapTokenManager.getTokenMap().size());
        assertEquals(token, coapTokenManager.getToken(clientId));
    }

    @Test
    public void testRemoveToken() {
        String clientId = "test111";

        String token = coapTokenManager.createToken(clientId);
        assertEquals(1, coapTokenManager.getTokenMap().size());

        coapTokenManager.removeToken(clientId);
        assertEquals(0, coapTokenManager.getTokenMap().size());
    }

    @Test
    public void testIsValid() {
        String clientId = "test111";
        String token = coapTokenManager.createToken(clientId);
        assertTrue(coapTokenManager.isValid(clientId, token));
    }

    @Test
    public void testNotValid() {
        String clientId = "test111";
        String token = coapTokenManager.createToken(clientId);
        assertFalse(coapTokenManager.isValid(clientId, "wrongToken"));
    }

}
