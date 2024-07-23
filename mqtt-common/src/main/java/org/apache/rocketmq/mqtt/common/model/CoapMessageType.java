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

public enum CoapMessageType {
    CON(0),
    NON(1),
    ACK(2),
    RST(3);

    private static final CoapMessageType[] VALUES;
    private final int value;

    private CoapMessageType(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    public static CoapMessageType valueOf(int type) {
        if (type >= 0 && type < VALUES.length) {
            return  VALUES[type];
        } else {
            throw new IllegalArgumentException("Unknown CoapMessageType " + type);
        }
    }

    static {
        CoapMessageType[] values = values();
        VALUES = new CoapMessageType[values.length + 1];

        for (CoapMessageType type : values) {
            int value = type.value;
            if (VALUES[value] != null) {
                throw new AssertionError("Value already in use: " + value + " by " + VALUES[value]);
            }
            VALUES[value] = type;
        }
    }

}
