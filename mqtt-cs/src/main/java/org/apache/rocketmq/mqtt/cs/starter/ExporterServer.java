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

package org.apache.rocketmq.mqtt.cs.starter;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.rocketmq.mqtt.common.util.HostInfo;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.exporter.MqttExporter;
import org.springframework.stereotype.Component;

@Component
public class ExporterServer {
    private static final HostInfo HOST_INFO = HostInfo.getInstall();
    private static final String NAMESPACE = "mqtt";
    @Resource
    private ConnectConf connectConf;

    private MqttExporter mqttExporter;

    @PostConstruct
    public void init() throws Exception {
        if (connectConf.isEnablePrometheus()) {
            this.mqttExporter = new MqttExporter(NAMESPACE, HOST_INFO.getName(), HOST_INFO.getAddress(),
                connectConf.getExporterPort(), connectConf.isExportJvmInfo());
            mqttExporter.start();
        }
    }

    @PreDestroy
    public void shutdown() {
        if (this.mqttExporter != null) {
            this.mqttExporter.shutdown();
        }
    }
}
