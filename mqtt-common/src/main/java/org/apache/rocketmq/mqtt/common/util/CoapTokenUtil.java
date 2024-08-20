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

package org.apache.rocketmq.mqtt.common.util;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CoapTokenUtil {

    private static final String ALGORITHM = "AES";
    private static final String SECRET_KEY = "mySecretKey12345";
    private static final long EXPIRATION_TIME = 60000;

    public static String generateToken(String clientId) throws Exception {
        long timestamp = System.currentTimeMillis();
        String data = clientId + ":" + timestamp;
        return encrypt(data, SECRET_KEY);
    }

    public static boolean isValid(String clientId, String token) {
        try {
            String decryptedData = decrypt(token, SECRET_KEY);
            String[] parts = decryptedData.split(":");
            if (parts.length != 2) {
                return false;
            }

            String decryptedClientId = parts[0];
            long timestamp = Long.parseLong(parts[1]);
            if (!decryptedClientId.equals(clientId)) {
                return false;
            }
            long currentTime = System.currentTimeMillis();
            return currentTime - timestamp <= EXPIRATION_TIME;

        } catch (Exception e) {
            return false;
        }
    }

    private static String encrypt(String data, String key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);

        byte[] encryptedBytes = cipher.doFinal(data.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    private static String decrypt(String token, String key) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKey);

        byte[] decryptedBytes = cipher.doFinal(Base64.getDecoder().decode(token));
        return new String(decryptedBytes, StandardCharsets.UTF_8);
    }
}
