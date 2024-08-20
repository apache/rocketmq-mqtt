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

public class CoapMessageOption {
    private CoapMessageOptionNumber optionNumber;
    private byte[] optionValue;

    public CoapMessageOption(CoapMessageOptionNumber optionNumber, byte[] optionValue) {
        this.optionNumber = optionNumber;
        this.optionValue = optionValue;
    }

    public CoapMessageOption(int optionNumber, byte[] optionValue) {
        this(CoapMessageOptionNumber.valueOf(optionNumber), optionValue);
    }

    public CoapMessageOptionNumber getOptionNumber() {
        return optionNumber;
    }

    public void setOptionNumber(CoapMessageOptionNumber optionNumber) {
        this.optionNumber = optionNumber;
    }

    public byte[] getOptionValue() {
        return optionValue;
    }

    public void setOptionValue(byte[] optionValue) {
        this.optionValue = optionValue;
    }
}
