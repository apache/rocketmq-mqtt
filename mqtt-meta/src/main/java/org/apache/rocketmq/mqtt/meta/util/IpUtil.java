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

package org.apache.rocketmq.mqtt.meta.util;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Set;

public class IpUtil {
    private static String candidatesHost;

    public static String getLocalAddressCompatible() {
        try {
            if (candidatesHost != null) {
                return candidatesHost;
            }
            return getLocalAddress();
        } catch (Exception e) {
            throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException", e);
        }
    }

    private static String getLocalAddress() throws Exception {
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
        ArrayList<String> ipv4Result = new ArrayList<String>();
        ArrayList<String> ipv6Result = new ArrayList<String>();
        while (enumeration.hasMoreElements()) {
            final NetworkInterface networkInterface = enumeration.nextElement();
            final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
            while (en.hasMoreElements()) {
                final InetAddress address = en.nextElement();
                if (!address.isLoopbackAddress()) {
                    if (address instanceof Inet6Address) {
                        ipv6Result.add(normalizeHostAddress(address));
                    } else {
                        ipv4Result.add(normalizeHostAddress(address));
                    }
                }
            }
        }

        if (!ipv4Result.isEmpty()) {
            for (String ip : ipv4Result) {
                if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
                    continue;
                }

                return ip;
            }

            return ipv4Result.get(ipv4Result.size() - 1);
        } else if (!ipv6Result.isEmpty()) {
            return ipv6Result.get(0);
        }
        final InetAddress localHost = InetAddress.getLocalHost();
        return normalizeHostAddress(localHost);
    }

    public static String getLocalPort() throws Exception {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Set<ObjectName> objectNames = mBeanServer.queryNames(new ObjectName("*:type=Connector,*"), null);
        if (objectNames == null || objectNames.size() <= 0) {
            throw new IllegalStateException("Cannot get the names of MBeans controlled by the MBean server.");
        }
        for (ObjectName objectName : objectNames) {
            String protocol = String.valueOf(mBeanServer.getAttribute(objectName, "protocol"));
            String port = String.valueOf(mBeanServer.getAttribute(objectName, "port"));
            if (protocol.equals("HTTP/1.1") || protocol.equals("org.apache.coyote.http11.Http11NioProtocol")) {
                return port;
            }
        }
        throw new IllegalStateException("Failed to get the HTTP port of the current server");
    }


    public static String normalizeHostAddress(final InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        } else {
            return localHost.getHostAddress();
        }
    }

    public static String convertAllNodeAddress(String ipList, int port) {
        StringBuilder allNodeAddress = new StringBuilder();
        String[] ips = ipList.split(",");
        for (int i = 0; i < ips.length - 1; ++i) {
            allNodeAddress.append(ips[i]).append(":").append(port).append(",");
        }
        allNodeAddress.append(ips[ips.length - 1]).append(":").append(port);
        return allNodeAddress.toString();
    }
}