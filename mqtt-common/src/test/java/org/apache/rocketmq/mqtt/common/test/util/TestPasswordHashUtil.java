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
package org.apache.rocketmq.mqtt.common.test.util;

import org.apache.rocketmq.mqtt.common.util.PasswordHashUtil;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestPasswordHashUtil {

    @Test
    public void testGenerateSalt() {
        int saltLength = 16;
        String salt = PasswordHashUtil.generateSalt(saltLength);
        assertEquals(saltLength, Base64.getDecoder().decode(salt).length);
    }

    @Test
    public void testHashWithSalt() throws NoSuchAlgorithmException {
        String password = "password";
        String salt = PasswordHashUtil.generateSalt(16);
        String algorithm = "SHA-256";
        String saltPosition = "suffix";

        String hashPassword = PasswordHashUtil.hashWithSalt(password, salt, algorithm, saltPosition);
        assertNotNull(hashPassword);
        assertNotEquals(password, hashPassword);

        String hashPassword2 = PasswordHashUtil.hashWithSalt(password, salt, algorithm, saltPosition);
        assertEquals(hashPassword, hashPassword2);
    }

    @Test
    public void testValidatePasswordPlain() throws Exception {
        String password = "password";
        assertTrue(PasswordHashUtil.validatePassword(password, password, null, "PLAIN", null));
    }

    @Test
    public void testValidatePasswordHash() throws Exception {
        String password = "password";
        String salt = PasswordHashUtil.generateSalt(16);
        String hashPassword = PasswordHashUtil.hashWithSalt(password, salt, "SHA-256", "suffix");
        assertTrue(PasswordHashUtil.validatePassword(password, hashPassword, salt, "SHA-256", "suffix"));
    }
}
