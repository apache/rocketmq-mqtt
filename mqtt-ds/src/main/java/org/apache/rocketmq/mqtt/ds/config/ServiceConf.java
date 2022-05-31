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

package org.apache.rocketmq.mqtt.ds.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Properties;

@Component
public class ServiceConf {
    private static final String CONF_FILE_NAME = "service.conf";
    private File confFile;
    private Properties properties;
    private int authThreadNum = 32;
    private int csRpcPort = 7001;
    private int eventNotifyRetryMaxTime = 3;
    private String eventNotifyRetryTopic;
    private String clientRetryTopic;
    private String clientP2pTopic;
    private String username;
    private String secretKey;

    private String clusterName = "defaultCluster";
    private String allNodeAddress;
    private String dbPath = "~/mqtt_meta/db/";
    private String raftDataPath = "~/mqtt_meta/raft/data";
    private int metaPort = 25000;
    public ServiceConf() throws IOException {
        ClassPathResource classPathResource = new ClassPathResource(CONF_FILE_NAME);
        InputStream in = classPathResource.getInputStream();
        Properties properties = new Properties();
        properties.load(in);
        in.close();
        this.properties = properties;
        MixAll.properties2Object(properties, this);
        this.confFile = new File(classPathResource.getURL().getFile());
        if (StringUtils.isBlank(clientRetryTopic)) {
            throw new RemoteException("clientRetryTopic is blank");
        }
        if (StringUtils.isBlank(eventNotifyRetryTopic)) {
            throw new RemoteException("eventNotifyRetryTopic is blank");
        }
    }

    public File getConfFile() {
        return confFile;
    }

    public int getAuthThreadNum() {
        return authThreadNum;
    }

    public void setAuthThreadNum(int authThreadNum) {
        this.authThreadNum = authThreadNum;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public int getCsRpcPort() {
        return csRpcPort;
    }

    public void setCsRpcPort(int csRpcPort) {
        this.csRpcPort = csRpcPort;
    }

    public int getEventNotifyRetryMaxTime() {
        return eventNotifyRetryMaxTime;
    }

    public void setEventNotifyRetryMaxTime(int eventNotifyRetryMaxTime) {
        this.eventNotifyRetryMaxTime = eventNotifyRetryMaxTime;
    }

    public String getEventNotifyRetryTopic() {
        return eventNotifyRetryTopic;
    }

    public void setEventNotifyRetryTopic(String eventNotifyRetryTopic) {
        this.eventNotifyRetryTopic = eventNotifyRetryTopic;
    }

    public String getClientRetryTopic() {
        return clientRetryTopic;
    }

    public void setClientRetryTopic(String clientRetryTopic) {
        this.clientRetryTopic = clientRetryTopic;
    }

    public String getClientP2pTopic() {
        return clientP2pTopic;
    }

    public void setClientP2pTopic(String clientP2pTopic) {
        this.clientP2pTopic = clientP2pTopic;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
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
}
