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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Component
public class ServiceConfListener {
    private static Logger logger = LoggerFactory.getLogger(ServiceConfListener.class);

    @Resource
    private ServiceConf serviceConf;

    private File confFile;
    private ScheduledThreadPoolExecutor scheduler;
    private AtomicLong gmt = new AtomicLong();
    private long refreshCheckInterval = 3;

    @PostConstruct
    public void start() {
        confFile = serviceConf.getConfFile();
        gmt.set(confFile.lastModified());
        scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("ServiceConfListener"));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                if (gmt.get() == confFile.lastModified()) {
                    return;
                }
                gmt.set(confFile.lastModified());
                InputStream in = new FileInputStream(confFile.getAbsoluteFile());
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                MixAll.properties2Object(properties, serviceConf);
                logger.warn("UpdateConf:{}", confFile.getAbsolutePath());
            } catch (Exception e) {
                logger.error("", e);
            }
        }, refreshCheckInterval, refreshCheckInterval, TimeUnit.SECONDS);
    }

}
