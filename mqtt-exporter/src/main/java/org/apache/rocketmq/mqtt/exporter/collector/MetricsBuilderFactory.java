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

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector.Builder;

public class MetricsBuilderFactory {

    private static Builder newGaugeBuilder(MqttMetricsInfo mqttMetricsInfo) {
        return Gauge.build().
            name(mqttMetricsInfo.getName()).
            help(mqttMetricsInfo.getHelp()).
            labelNames(mqttMetricsInfo.getLabelNames());

    }

    private static Builder newCounterBuilder(MqttMetricsInfo mqttMetricsInfo) {
        return Counter.build().
            name(mqttMetricsInfo.getName()).
            help(mqttMetricsInfo.getHelp()).
            labelNames(mqttMetricsInfo.getLabelNames());
    }

    private static Builder newHistogramBuilder(MqttMetricsInfo mqttMetricsInfo) {
        return Histogram.build().
            name(mqttMetricsInfo.getName()).
            help(mqttMetricsInfo.getHelp()).
            labelNames(mqttMetricsInfo.getLabelNames()).
            buckets(mqttMetricsInfo.getBuckets());
    }

    public static Builder newCollectorBuilder(MqttMetricsInfo mqttMetricsInfo) {
        switch (mqttMetricsInfo.getType()) {
            case COUNTER:
                return newCounterBuilder(mqttMetricsInfo);
            case GAUGE:
                return newGaugeBuilder(mqttMetricsInfo);
            case HISTOGRAM:
                return newHistogramBuilder(mqttMetricsInfo);
            default:
                break;

        }
        return null;
    }

}
