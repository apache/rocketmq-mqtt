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

import java.net.InetAddress;
import java.net.UnknownHostException;

public class HostInfo {
    private final String hostName;
    private final String hostAddress;

    private static final HostInfo INSTALL = new HostInfo();

    public static HostInfo getInstall() {
        return INSTALL;
    }

    private HostInfo() {
        String hostName;
        String hostAddress;
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            hostName = localhost.getHostName();
            hostAddress = localhost.getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        this.hostName = hostName;
        this.hostAddress = hostAddress;
    }

    public final String getName() {
        return this.hostName;
    }

    public final String getAddress() {
        return this.hostAddress;
    }
}
