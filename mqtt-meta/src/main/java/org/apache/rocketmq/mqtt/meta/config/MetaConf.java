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

package org.apache.rocketmq.mqtt.meta.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.rocketmq.common.MixAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component
public class MetaConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaConf.class);
    private static final String CONF_FILE_NAME = "meta.conf";
    private File confFile;
    private String allNodeAddress;
    private int metaPort = 25000;
    private String selfAddress;
    private int raftNodePort = 8081;
    private String membersAddress;
    private int maxRetainedTopicNum = 10000;
    private int electionTimeoutMs = 1000;
    private int snapshotIntervalSecs = 60 * 1000;
    private String raftServiceName = System.getenv("RaftServiceName");

    private int scanNum = 10000;

    public MetaConf() throws IOException {
        try {
            ClassPathResource classPathResource = new ClassPathResource(CONF_FILE_NAME);
            InputStream in = classPathResource.getInputStream();
            Properties properties = new Properties();
            properties.load(in);
            in.close();
            MixAll.properties2Object(properties, this);
            this.confFile = new File(classPathResource.getURL().getFile());
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public File getConfFile() {
        return confFile;
    }

    public String getAllNodeAddress() {
        return allNodeAddress;
    }

    public void setAllNodeAddress(String allNodeAddress) {
        this.allNodeAddress = allNodeAddress;
    }

    public int getMetaPort() {
        return metaPort;
    }

    public void setMetaPort(int metaPort) {
        this.metaPort = metaPort;
    }

    public String getSelfAddress() {
        return selfAddress;
    }

    public void setSelfAddress(String selfAddress) {
        this.selfAddress = selfAddress;
    }

    public String getMembersAddress() {
        return membersAddress;
    }

    public void setMembersAddress(String membersAddress) {
        this.membersAddress = membersAddress;
    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public int getSnapshotIntervalSecs() {
        return snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    public int getMaxRetainedTopicNum() {
        return maxRetainedTopicNum;
    }

    public void setMaxRetainedTopicNum(int maxRetainedTopicNum) {
        this.maxRetainedTopicNum = maxRetainedTopicNum;
    }

    public String getRaftServiceName() {
        return raftServiceName;
    }

    public void setRaftServiceName(String raftServiceName) {
        this.raftServiceName = raftServiceName;
    }

    public int getRaftNodePort() {
        return raftNodePort;
    }

    public void setRaftNodePort(int raftNodePort) {
        this.raftNodePort = raftNodePort;
    }

    public int getScanNum() {
        return scanNum;
    }

    public void setScanNum(int scanNum) {
        this.scanNum = scanNum;
    }
}
