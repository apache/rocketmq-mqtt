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

package org.apache.rocketmq.mqtt.cs.config;

import org.apache.rocketmq.common.MixAll;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Component
public class ConnectConf {
    private static final String CONF_FILE_NAME = "connect.conf";
    private File confFile;
    private int nettySelectThreadNum = 1;
    private int nettyWorkerThreadNum = Runtime.getRuntime().availableProcessors() * 2;
    private int mqttPort = 1883;
    private int mqttWsPort = 8888;
    private int maxPacketSizeInByte = 64 * 1024;
    private int highWater = 256 * 1024;
    private int lowWater = 16 * 1024;
    private int maxConn = 10 * 10000;
    private boolean order;
    private int maxRetryTime = 15;
    private int sizeOfNotRollWhenAckSlow = 32;
    private int queueCacheSize = 128;
    private int pullBatchSize = 32;
    private int rpcListenPort = 7001;
    private int retryIntervalSeconds = 3;
    private int exporterPort = 9090;
    private boolean enablePrometheus = false;
    private boolean exportJvmInfo = true;

    public ConnectConf() throws IOException {
        ClassPathResource classPathResource = new ClassPathResource(CONF_FILE_NAME);
        InputStream in = classPathResource.getInputStream();
        Properties properties = new Properties();
        properties.load(in);
        in.close();
        MixAll.properties2Object(properties, this);
        this.confFile = new File(classPathResource.getURL().getFile());
    }

    public File getConfFile() {
        return confFile;
    }

    public int getNettySelectThreadNum() {
        return nettySelectThreadNum;
    }

    public void setNettySelectThreadNum(int nettySelectThreadNum) {
        this.nettySelectThreadNum = nettySelectThreadNum;
    }

    public int getNettyWorkerThreadNum() {
        return nettyWorkerThreadNum;
    }

    public void setNettyWorkerThreadNum(int nettyWorkerThreadNum) {
        this.nettyWorkerThreadNum = nettyWorkerThreadNum;
    }

    public int getMqttPort() {
        return mqttPort;
    }

    public void setMqttPort(int mqttPort) {
        this.mqttPort = mqttPort;
    }

    public int getMqttWsPort() {
        return mqttWsPort;
    }

    public void setMqttWsPort(int mqttWsPort) {
        this.mqttWsPort = mqttWsPort;
    }

    public int getMaxPacketSizeInByte() {
        return maxPacketSizeInByte;
    }

    public void setMaxPacketSizeInByte(int maxPacketSizeInByte) {
        this.maxPacketSizeInByte = maxPacketSizeInByte;
    }

    public int getHighWater() {
        return highWater;
    }

    public void setHighWater(int highWater) {
        this.highWater = highWater;
    }

    public int getLowWater() {
        return lowWater;
    }

    public void setLowWater(int lowWater) {
        this.lowWater = lowWater;
    }

    public int getMaxConn() {
        return maxConn;
    }

    public void setMaxConn(int maxConn) {
        this.maxConn = maxConn;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public int getMaxRetryTime() {
        return maxRetryTime;
    }

    public void setMaxRetryTime(int maxRetryTime) {
        this.maxRetryTime = maxRetryTime;
    }

    public int getSizeOfNotRollWhenAckSlow() {
        return sizeOfNotRollWhenAckSlow;
    }

    public void setSizeOfNotRollWhenAckSlow(int sizeOfNotRollWhenAckSlow) {
        this.sizeOfNotRollWhenAckSlow = sizeOfNotRollWhenAckSlow;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public int getQueueCacheSize() {
        return queueCacheSize;
    }

    public void setQueueCacheSize(int queueCacheSize) {
        this.queueCacheSize = queueCacheSize;
    }

    public int getRpcListenPort() {
        return rpcListenPort;
    }

    public void setRpcListenPort(int rpcListenPort) {
        this.rpcListenPort = rpcListenPort;
    }

    public int getRetryIntervalSeconds() {
        return retryIntervalSeconds;
    }

    public void setRetryIntervalSeconds(int retryIntervalSeconds) {
        this.retryIntervalSeconds = retryIntervalSeconds;
    }

    public int getExporterPort() {
        return exporterPort;
    }

    public void setExporterPort(int exporterPort) {
        this.exporterPort = exporterPort;
    }

    public boolean isEnablePrometheus() {
        return enablePrometheus;
    }

    public void setEnablePrometheus(boolean enablePrometheus) {
        this.enablePrometheus = enablePrometheus;
    }

    public boolean isExportJvmInfo() {
        return exportJvmInfo;
    }

    public void setExportJvmInfo(boolean exportJvmInfo) {
        this.exportJvmInfo = exportJvmInfo;
    }
}
