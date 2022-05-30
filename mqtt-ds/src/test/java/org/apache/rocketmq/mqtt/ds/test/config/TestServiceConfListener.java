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

package org.apache.rocketmq.mqtt.ds.test.config;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.config.ServiceConfListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestServiceConfListener {

    @Mock
    private ServiceConf serviceConf;

    @Mock
    private File confFile;

    private ServiceConfListener confListener;
    private final long firstModifiedTime = 1000;
    private final long secondModifiedTime = 2000;

    @Test
    public void test() throws Exception {
        confListener = new ServiceConfListener();
        FieldUtils.writeDeclaredField(confListener, "serviceConf", serviceConf, true);
        FieldUtils.writeDeclaredField(confListener, "confFile", confFile, true);
        FieldUtils.writeDeclaredField(confListener, "refreshCheckInterval", 1, true);

        when(serviceConf.getConfFile()).thenReturn(confFile);
        when(confFile.lastModified()).thenReturn(firstModifiedTime, secondModifiedTime);
        when(confFile.getAbsoluteFile()).thenReturn(File.createTempFile("temp", ".properties"));

        confListener.start();
        Thread.sleep(1100);

        verify(serviceConf).getConfFile();
        verify(confFile, times(3)).lastModified();
        verify(confFile).getAbsoluteFile();
    }
}
