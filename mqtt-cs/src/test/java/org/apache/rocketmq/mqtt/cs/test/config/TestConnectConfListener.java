/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.cs.test.config;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.config.ConnectConfListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestConnectConfListener {

    private ConnectConfListener confListener;
    private final long confRefreshSecs = 1;
    private final long originalModify = 1000L;

    @Mock
    private ConnectConf connectConf;

    @Mock
    private File confFile;

    @Before
    public void Before() throws IllegalAccessException {
        confListener = new ConnectConfListener();
        FieldUtils.writeDeclaredField(confListener, "connectConf", connectConf, true);
        FieldUtils.writeDeclaredField(confListener, "confFile", confFile, true);
        FieldUtils.writeDeclaredField(confListener, "refreshSecs", confRefreshSecs, true);
    }

    @After
    public void After() {}

    @Test
    public void testStart() {
        when(connectConf.getConfFile()).thenReturn(confFile);
        when(confFile.lastModified()).thenReturn(originalModify);

        confListener.start();
        verify(confFile, times(1)).lastModified();
        verifyNoMoreInteractions(confFile);

        // wait the next conf refresh check and make conf modified
        when(confFile.lastModified()).thenReturn(2 * originalModify);
        try {
            Thread.sleep(2000 + confRefreshSecs);
        } catch (InterruptedException ignored) {}

        verify(confFile, atLeast(2)).lastModified();
        verify(confFile, atLeast(1)).getAbsoluteFile();
    }
}
