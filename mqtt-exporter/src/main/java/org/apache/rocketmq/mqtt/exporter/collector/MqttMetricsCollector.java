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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector.Builder;
import org.apache.rocketmq.mqtt.exporter.exception.PrometheusException;
import org.apache.rocketmq.mqtt.exporter.http.MqttHTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttMetricsCollector {
    public static final CollectorRegistry COLLECTOR_REGISTRY = CollectorRegistry.defaultRegistry;
    private static final Logger LOG = LoggerFactory.getLogger(MqttMetricsCollector.class);
    private static final Map<Type, Map<MqttMetricsInfo, Collector>> ALL_TYPE_COLLECTORS = new ConcurrentHashMap<>();
    private static volatile boolean initialized = false;
    private static String namespace;
    private static String hostName;
    private static String hostIp;
    private static MqttHTTPServer httpServer;

    public static synchronized void initialize(String nameSpace, String hostName, String hostIp, int exporterPort) throws IOException {
        if (!initialized) {
            MqttMetricsCollector.namespace = nameSpace;
            MqttMetricsCollector.hostName = hostName;
            MqttMetricsCollector.hostIp = hostIp;
            register(MqttMetricsInfo.values());
            httpServer = new MqttHTTPServer(new InetSocketAddress(exporterPort), MqttMetricsCollector.COLLECTOR_REGISTRY, false);
            initialized = true;
        }
    }

    private static void register(MqttMetricsInfo[] mqttMetricsInfos) {
        for (MqttMetricsInfo metricsInfo : mqttMetricsInfos) {
            register(metricsInfo);
        }
    }

    public static void register(MqttMetricsInfo metricsInfo) {
        Map<MqttMetricsInfo, Collector> mqttMetricsInfoCollectorTypeMap = ALL_TYPE_COLLECTORS.get(metricsInfo.getType());
        if (mqttMetricsInfoCollectorTypeMap == null) {
            mqttMetricsInfoCollectorTypeMap = new ConcurrentHashMap<>();
            Map<MqttMetricsInfo, Collector> oldMap = ALL_TYPE_COLLECTORS.putIfAbsent(metricsInfo.getType(),
                mqttMetricsInfoCollectorTypeMap);

            if (oldMap != null) {
                mqttMetricsInfoCollectorTypeMap = oldMap;
            }
        }
        if (mqttMetricsInfoCollectorTypeMap.get(metricsInfo) == null) {
            Builder builder = MetricsBuilderFactory.newCollectorBuilder(metricsInfo);
            if (builder == null) {
                return;
            }
            mqttMetricsInfoCollectorTypeMap.putIfAbsent(metricsInfo, buildNameSpace(builder, metricsInfo).register(COLLECTOR_REGISTRY));
        }
    }

    public static void unRegister(MqttMetricsInfo metricsInfo) {
        Map<MqttMetricsInfo, Collector> mqttMetricsInfoCollectorTypeMap = ALL_TYPE_COLLECTORS.get(metricsInfo.getType());
        if (mqttMetricsInfoCollectorTypeMap == null) {
            return;
        }
        Collector collector = mqttMetricsInfoCollectorTypeMap.get(metricsInfo);
        if (collector == null) {
            return;
        }
        COLLECTOR_REGISTRY.unregister(collector);
        mqttMetricsInfoCollectorTypeMap.remove(metricsInfo, collector);

    }

    private static Builder buildNameSpace(Builder builder, MqttMetricsInfo mqttMetricsInfo) {
        return builder.namespace(namespace).subsystem(mqttMetricsInfo.getSubSystem().getValue());
    }

    private static String[] paddingClusterLabelValues(String... labelValues) {
        String[] newLabelValues = new String[labelValues.length + 2];
        int index = 0;
        newLabelValues[index++] = hostName;
        newLabelValues[index++] = hostIp;
        for (int i = 0; i < labelValues.length; i++, index++) {
            newLabelValues[index] = labelValues[i];
        }
        return newLabelValues;
    }

    private static void collect(MqttMetricsInfo mqttMetricsInfo, long val, String... labels) throws PrometheusException {
        if (!initialized) {
            return;
        }

        Map<MqttMetricsInfo, Collector> mqttMetricsInfoCollectorTypeMap = ALL_TYPE_COLLECTORS.get(mqttMetricsInfo.getType());
        if (mqttMetricsInfoCollectorTypeMap == null) {
            throw new PrometheusException("mqttMetricsInfo unregistered or collector type not support: " + mqttMetricsInfo);
        }
        Collector collector = mqttMetricsInfoCollectorTypeMap.get(mqttMetricsInfo);
        if (collector == null) {
            throw new PrometheusException("mqttMetricsInfo unregistered or collector type not support: " + mqttMetricsInfo);
        }

        try {
            if (mqttMetricsInfo.getType() == Type.COUNTER) {
                Counter counter = (Counter)collector;
                counter.labels(paddingClusterLabelValues(labels)).inc(val);
            } else if (mqttMetricsInfo.getType() == Type.GAUGE) {
                Gauge gauge = (Gauge)collector;
                gauge.labels(paddingClusterLabelValues(labels)).set(val);
            } else if (mqttMetricsInfo.getType() == Type.HISTOGRAM) {
                Histogram histogram = (Histogram)collector;
                histogram.labels(paddingClusterLabelValues(labels)).observe(val);
            }
        } catch (Exception e) {
            LOG.error("collect metrics exception.", labels2String(labels), val, e);
            throw new PrometheusException("collect metrics exception", e);
        }
    }

    public static void collectDemoTps(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.DEMO_TPS, val, labels);
    }

    public static void collectDemoGuage(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.DEMO_GAUGE, val, labels);
    }

    public static void collectDemoLatency(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.DEMO_LATENCY, val, labels);
    }

    public static void collectPullStatusTps(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.PULL_STATUS_TPS, val, labels);
    }

    public static void collectPullCacheStatusTps(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.PULL_CACHE_STATUS_TPS, val, labels);
    }

    public static void collectLmqReadWriteMatchActionRt(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.READ_WRITE_MATCH_ACTION_RT, val, labels);
    }

    public static void collectConnectionsSize(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.CONNECTIONS_SIZE, val, labels);
    }

    public static void collectReadWriteMatchActionBytes(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.READ_WRITE_MATCH_ACTION_BYTES, val, labels);
    }

    public static void collectProcessRequestTps(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.PROCESS_REQUEST_TPS, val, labels);
    }

    public static void collectPutRequestTps(long val, String... labels) throws PrometheusException {
        collect(MqttMetricsInfo.PUT_REQUEST_TPS, val, labels);
    }

    private static String labels2String(String... labels) {
        StringBuilder sb = new StringBuilder(128);
        for (String label : labels) {
            sb.append(label).append(";");
        }
        return sb.toString();
    }

    public static void shutdown() {
        if (httpServer != null) {
            httpServer.close();
        }
    }
}
