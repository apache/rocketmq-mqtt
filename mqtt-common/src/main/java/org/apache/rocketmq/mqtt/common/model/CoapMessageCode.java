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

public enum CoapMessageCode {
    // Request Code, 0.xx
    GET(1),
    POST(2),
    PUT(3),
    DELETE(4),

    // Response Success Code, 2.xx
    CREATED(65),
    DELETED(66),
    Valid(67),
    CHANGED(68),
    CONTENT(69),

    // Response Client Error Code, 4.xx
    BAD_REQUEST(128),
    UNAUTHORIZED(129),
    BAD_OPTION(130),
    FORBIDDEN(131),
    NOT_FOUND(132),
    METHOD_NOT_ALLOWED(133),
    NOT_ACCEPTABLE(134),
    PRECONDITION_FAILED(140),
    REQUEST_ENTITY_TOO_LARGE(141),
    UNSUPPORTED_CONTENT_FORMAT(143),

    // Response Server Error Code, 5.xx
    INTERNAL_SERVER_ERROR(160),
    NOT_IMPLEMENTED(161),
    BAD_GATEWAY(162),
    SERVICE_UNAVAILABLE(163),
    GATEWAY_TIMEOUT(164),
    PROXYING_NOT_SUPPORTED(165);

    private static final CoapMessageCode[] VALUES;
    private final int value;

    private CoapMessageCode(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static CoapMessageCode valueOf(int code) {
        if (code > 0 && code < VALUES.length && VALUES[code] != null) {
            return  VALUES[code];
        } else {
            throw new IllegalArgumentException("Unknown CoapMessageCode " + code);
        }
    }

    public static boolean isRequestCode(CoapMessageCode code) {
        return (code == GET) || (code == POST) || (code == PUT) || (code == DELETE);
    }

    static {
        CoapMessageCode[] values = values();
        VALUES = new CoapMessageCode[192];  // Using 192 since the highest defined code is 192

        for (CoapMessageCode code : values) {
            int value = code.value;
            if (VALUES[value] != null) {
                throw new AssertionError("Value already in use: " + value + " by " + VALUES[value]);
            }
            VALUES[value] = code;
        }
    }


}
