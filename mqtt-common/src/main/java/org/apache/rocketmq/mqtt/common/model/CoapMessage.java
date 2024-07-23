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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CoapMessage {
    private int version;
    private CoapMessageType type;
    private int tokenLength;
    private CoapMessageCode code;
    private int messageId;
    private byte[] token;
    private List<CoapMessageOption> options = new ArrayList<>();
    private byte[] payload;
    private InetSocketAddress remoteAddress;

    public CoapMessage(int version, CoapMessageType type, int tokenLength, CoapMessageCode code, int messageId, byte[] token, byte[] payload, InetSocketAddress remoteAddress) {
        this.version = version;
        this.type = type;
        this.tokenLength = tokenLength;
        this.code = code;
        this.messageId = messageId;
        this.token = token;
        this.payload = payload;
        this.remoteAddress = remoteAddress;
    }

    public CoapMessage(int version, CoapMessageType type, int tokenLength, CoapMessageCode code, int messageId, byte[] token, InetSocketAddress remoteAddress) {
        this.version = version;
        this.type = type;
        this.tokenLength = tokenLength;
        this.code = code;
        this.messageId = messageId;
        this.token = token;
        this.remoteAddress = remoteAddress;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public CoapMessageType getType() {
        return type;
    }

    public void setType(CoapMessageType type) {
        this.type = type;
    }

    public int getTokenLength() {
        return tokenLength;
    }

    public void setTokenLength(int tokenLength) {
        this.tokenLength = tokenLength;
    }

    public CoapMessageCode getCode() {
        return code;
    }

    public void setCode(CoapMessageCode code) {
        this.code = code;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
    }

    public List<CoapMessageOption> getOptions() {
        return options;
    }

    public void setOptions(List<CoapMessageOption> options) {
        this.options = options;
    }

    public void addOption(CoapMessageOption option) {
        this.options.add(option);
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public void setRemoteAddress(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    @Override
    public String toString() {
        return "CoapMessage{" +
                "version=" + version +
                ", type=" + type +
                ", tokenLength=" + tokenLength +
                ", code=" + code +
                ", messageId=" + messageId +
                ", token=" + Arrays.toString(token) +
                ", options=" + options +
                ", payload=" + Arrays.toString(payload) +
                ", remoteAddress=" + remoteAddress +
                '}';
    }
}
