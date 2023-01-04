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

package org.apache.rocketmq.mqtt.ds.test.meta;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.meta.WillMsgClient;
import org.apache.rocketmq.mqtt.ds.meta.WillMsgPersistManagerImpl;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
public class WillMsgPersistManagerImplTest {
    public WillMsgPersistManagerImpl willMsgPersistManager;
    public WillMsgClient willMsgClient;
    private long checkAliveIntervalMillis = 5 * 1000;

    @Before
    public void Before() throws IOException, IllegalAccessException, InterruptedException, TimeoutException {
        willMsgClient = new WillMsgClient();
        ServiceConf serviceConf = mock(ServiceConf.class);
        when(serviceConf.getMetaAddr()).thenReturn("");
        FieldUtils.writeDeclaredField(willMsgClient, "serviceConf", serviceConf, true);

        willMsgClient.init();
        willMsgPersistManager = new WillMsgPersistManagerImpl();
        FieldUtils.writeDeclaredField(willMsgPersistManager, "willMsgClient", willMsgClient, true);
    }

    @Test
    public void put() throws ExecutionException, InterruptedException, TimeoutException {
        String ip = IpUtil.getLocalAddressCompatible();
        String csKey = Constants.CS_ALIVE + Constants.CTRL_1 + ip;
        long currentTime = System.currentTimeMillis();

        CompletableFuture<Boolean> future = willMsgPersistManager.put(csKey, String.valueOf(currentTime));
        Assert.assertTrue(future.get(3000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void get() throws ExecutionException, InterruptedException, TimeoutException {
        String ip = IpUtil.getLocalAddressCompatible();
        String csKey = Constants.CS_ALIVE + Constants.CTRL_1 + ip;
        long currentTime = System.currentTimeMillis();

        CompletableFuture<Boolean> future = willMsgPersistManager.put(csKey, String.valueOf(currentTime));
        Assert.assertTrue(future.get(3000, TimeUnit.MILLISECONDS));

        CompletableFuture<byte[]> future1 = willMsgPersistManager.get(csKey);
        Assert.assertEquals(String.valueOf(currentTime), new String(future1.get(3000, TimeUnit.MILLISECONDS)));
    }

    @Test
    public void compareAndPut() throws ExecutionException, InterruptedException, TimeoutException {
        String ip = IpUtil.getLocalAddressCompatible();
        String csKey = Constants.CS_ALIVE + Constants.CTRL_1 + ip;
        String masterKey = Constants.CS_MASTER;
        long currentTime = System.currentTimeMillis();

        willMsgPersistManager.get(masterKey).whenComplete((result, throwable) -> {
            String content = new String(result);
            if (Constants.NOT_FOUND.equals(content) || masterHasDown(content)) {
                willMsgPersistManager.compareAndPut(masterKey, content, ip + Constants.COLON + currentTime).whenComplete((rs, tb) -> {
                    if (!rs || tb != null) {
                        System.out.println("{} fail to update master" + ip);
                        return;
                    }
                    System.out.println("------------put success-------------------");
                });

            }
        });

        Thread.sleep(10000);
    }

    private boolean masterHasDown(String masterValue) {
        String[] ipTime = masterValue.split(Constants.COLON);
        if (ipTime.length < 2) {
            return true;
        }

        return System.currentTimeMillis() - Long.parseLong(ipTime[1]) > 10 * checkAliveIntervalMillis;
    }

    @Test
    public void scan() throws ExecutionException, InterruptedException, TimeoutException {
        String ip = "xxxx";
        String startClientKey = ip + Constants.CTRL_0;
        String endClientKey = ip + Constants.CTRL_2;
        willMsgPersistManager.scan(startClientKey, endClientKey).whenComplete((willMap, throwable) -> {
            if (willMap == null || throwable != null) {
                return;
            }

            if (willMap.size() == 0) {
                return;
            }
        });
        Thread.sleep(10000);


    }



}
