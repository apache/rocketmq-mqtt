package org.apache.rocketmq.mqtt.meta.util;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */
public class IpUtil {
    public static String getLocalAddressCompatible() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            try {
                String candidatesHost = getLocalAddress();
                if (candidatesHost != null) {
                    return candidatesHost;
                }

            } catch (Exception ignored) {
            }

            throw new RuntimeException("InetAddress java.net.InetAddress.getLocalHost() throws UnknownHostException",
                e);
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
        for (int i = 0; i < ips.length-1; ++i) {
            allNodeAddress.append(ips[i]).append(":").append(port).append(",");
        }
        allNodeAddress.append(ips[ips.length-1]).append(":").append(port);
        return allNodeAddress.toString();
    }
}
