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

package org.apache.rocketmq.mqtt.exporter.collector;

import java.util.Arrays;

import io.prometheus.client.Collector.Type;

public enum MqttMetricsInfo {
    DEMO_TPS(Type.COUNTER, SubSystem.CS, "tps_total", "demo tps.", null,
        "hostName", "hostIp"),
    DEMO_GAUGE(Type.GAUGE, SubSystem.DS,"demo_gauge", "demo gauge status.",
        null,
        "hostName", "hostIp"),
    DEMO_LATENCY(Type.HISTOGRAM, SubSystem.DS,"demo_latency", "latency in microsecond.",
        new double[] {100, 300, 500, 1000, 3000, 5000, 10000, 50000},
        "hostName", "hostIp"),
    PULL_STATUS_TPS(Type.COUNTER, SubSystem.DS, "pull_status_tps_total", "ds pull msg status tps.", null,
        "hostName", "hostIp", "pullStatus"),
    PULL_CACHE_STATUS_TPS(Type.COUNTER, SubSystem.CS, "pull_cache_status_tps_total", "ds pull cache status tps.", null,
        "hostName", "hostIp", "pullCacheStatus"),
    READ_WRITE_MATCH_ACTION_RT(Type.GAUGE, SubSystem.DS, "read_write_match_action_rt", "lmq read write match action rt.", null,
        "hostName", "hostIp", "action", "status"),
    CONNECTIONS_SIZE(Type.GAUGE, SubSystem.CS, "connections_size", "server connections size.", null,
        "hostName", "hostIp"),
    READ_WRITE_MATCH_ACTION_BYTES(Type.COUNTER, SubSystem.DS, "read_write_match_action_bytes", "lmq read write match action bytes.", null,
        "hostName", "hostIp", "topic", "action"),
    PROCESS_REQUEST_TPS(Type.COUNTER, SubSystem.DS, "process_request_tps", "ds process request tps.", null,
        "hostName", "hostIp", "messageType"),
    PUT_REQUEST_TPS(Type.COUNTER, SubSystem.DS, "put_request_tps", "ds topic put request tps.", null,
        "hostName", "hostIp", "topic");


    private final Type type;
    private final SubSystem subSystem;
    private final String name;
    private final String help;
    private final double[] buckets;
    private final String[] labelNames;

    MqttMetricsInfo(Type type, SubSystem subSystem, String name, String help, double[] buckets, String... labelNames) {
        this.type = type;
        this.subSystem = subSystem;
        this.name = name;
        this.help = help;
        this.buckets = buckets;
        this.labelNames = labelNames;
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getHelp() {
        return help;
    }

    public double[] getBuckets() {
        return buckets;
    }

    public String[] getLabelNames() {
        return labelNames;
    }

    public SubSystem getSubSystem() {
        return subSystem;
    }

    @Override
    public String toString() {
        return "MqttMetricsInfo{" +
            "type=" + type +
            ", subSystem=" + subSystem +
            ", name='" + name + '\'' +
            ", help='" + help + '\'' +
            ", buckets=" + Arrays.toString(buckets) +
            ", labelNames=" + Arrays.toString(labelNames) +
            '}';
    }
}