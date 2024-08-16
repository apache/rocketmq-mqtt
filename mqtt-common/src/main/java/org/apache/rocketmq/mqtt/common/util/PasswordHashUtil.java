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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class PasswordHashUtil {

    // Generate random salt.
    public static String generateSalt(int length) {
        byte[] salt = new byte[length];
        new SecureRandom().nextBytes(salt);
        return Base64.getEncoder().encodeToString(salt);
    }

    // Hash with salt, algorithm must be String of "MD5"/"SHA"/"SHA-256"/"SHA-512".
    public static String hashWithSalt(String password, String salt, String algorithm, String saltPosition) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance(algorithm);
        switch (saltPosition) {
            case "prefix":
                md.update(salt.getBytes(StandardCharsets.UTF_8));
                md.update(password.getBytes(StandardCharsets.UTF_8));
                break;
            case "suffix":
                md.update(password.getBytes(StandardCharsets.UTF_8));
                md.update(salt.getBytes(StandardCharsets.UTF_8));
                break;
            case "disable":
                md.update(password.getBytes(StandardCharsets.UTF_8));
            default:
                throw new IllegalArgumentException("Invalidsalt position: " + saltPosition);
        }
        byte[] hashedPassword = md.digest();
        return Base64.getEncoder().encodeToString(hashedPassword);
    }


    public static boolean validatePassword(String password, String storedHash, String salt, String algorithm, String saltPosition) throws NoSuchAlgorithmException {
        if (algorithm.equals("PLAIN")) {
            return password.equals(storedHash);
        }
        return hashWithSalt(password, salt, algorithm, saltPosition).equals(storedHash);
    }


}
