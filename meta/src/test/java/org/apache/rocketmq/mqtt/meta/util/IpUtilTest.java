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

package org.apache.rocketmq.mqtt.meta.util;

import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */

@RunWith(MockitoJUnitRunner.class)
public class IpUtilTest {
    @Mock
    private MetaConf serviceConf;

    @Test
    public void convertAllNodeAddressTest() {
        when(serviceConf.getAllNodeAddress()).thenReturn("127.0.0.1");
        when(serviceConf.getMetaPort()).thenReturn(25000);
        String allNodes = IpUtil.convertAllNodeAddress(serviceConf.getAllNodeAddress(), serviceConf.getMetaPort());
        Assert.assertEquals("127.0.0.1:25000", allNodes);

        when(serviceConf.getAllNodeAddress()).thenReturn("127.0.0.1,127.0.0.2");
        when(serviceConf.getMetaPort()).thenReturn(25000);
        String allNodes1 = IpUtil.convertAllNodeAddress(serviceConf.getAllNodeAddress(), serviceConf.getMetaPort());
        Assert.assertEquals("127.0.0.1:25000,127.0.0.2:25000", allNodes1);
    }

    @Test
    public void getLocalAddressCompatible() {
        String ip = IpUtil.getLocalAddressCompatible();
        Assert.assertNotNull(ip);
    }

}
