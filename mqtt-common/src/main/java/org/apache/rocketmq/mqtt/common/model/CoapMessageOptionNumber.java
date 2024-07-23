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
package org.apache.rocketmq.mqtt.common.model;

public enum CoapMessageOptionNumber {
    IF_MATCH(1),
    URI_HOST(3),
    ETAG(4),
    IF_NONE_MATCH(5),
    OBSERVE(6),
    URI_PORT(7),
    LOCATION_PATH(8),
    URI_PATH(11),
    CONTENT_FORMAT(12),
    MAX_AGE(14),
    URI_QUERY(15),
    ACCEPT(17),
    LOCATION_QUERY(20),
    BLOCK_2(23),
    BLOCK_1(27),
    SIZE_2(28),
    PROXY_URI(35),
    PROXY_SCHEME(39),
    SIZE_1(60),
    REQUST_TAG(292);

    private static final CoapMessageOptionNumber[] VALUES;
    private final int value;

    private CoapMessageOptionNumber(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static boolean isValid(int number) {
        return number > 0 && number < 293 && VALUES[number] != null;
    }

    public static CoapMessageOptionNumber valueOf(int number) {
        if (number > 0 && number < 293 && VALUES[number] != null) {
            return  VALUES[number];
        } else {
            throw new IllegalArgumentException("Unknown CoapMessageOptionNumber " + number);
        }
    }

    static {
        CoapMessageOptionNumber[] values = values();
        VALUES = new CoapMessageOptionNumber[293];

        for (CoapMessageOptionNumber number : values) {
            int value = number.value;
            if (VALUES[value] != null) {
                throw new AssertionError("Value already in use: " + value + " by " + VALUES[value]);
            }
            VALUES[value] = number;
        }
    }
}
