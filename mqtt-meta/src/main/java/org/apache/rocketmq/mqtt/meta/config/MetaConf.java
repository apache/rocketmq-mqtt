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
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component
public class MetaConf {
    private static final String CONF_FILE_NAME = "meta.conf";
    private File confFile;
    private String clusterName = "defaultCluster";
    private String allNodeAddress;
    private String dbPath = System.getProperty("user.home") + "/mqtt_meta/db";
    private String raftDataPath = System.getProperty("user.home") + "/mqtt_meta/raft";
    private int metaPort = 25000;
    private String selfAddress;
    private String membersAddress;
    private int maxRetainedMessageNum;
    private int electionTimeoutMs = 1000;
    private int snapshotIntervalSecs = 1000;
    private String raftServiceName = System.getenv("RaftServiceName");

    public MetaConf() throws IOException {
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

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getAllNodeAddress() {
        return allNodeAddress;
    }

    public void setAllNodeAddress(String allNodeAddress) {
        this.allNodeAddress = allNodeAddress;
    }

    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    public String getRaftDataPath() {
        return raftDataPath;
    }

    public void setRaftDataPath(String raftDataPath) {
        this.raftDataPath = raftDataPath;
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

    public int getMaxRetainedMessageNum() {
        return maxRetainedMessageNum;
    }

    public void setMaxRetainedMessageNum(int maxRetainedMessageNum) {
        this.maxRetainedMessageNum = maxRetainedMessageNum;
    }

    public String getRaftServiceName() {
        return raftServiceName;
    }

    public void setRaftServiceName(String raftServiceName) {
        this.raftServiceName = raftServiceName;
    }
}
