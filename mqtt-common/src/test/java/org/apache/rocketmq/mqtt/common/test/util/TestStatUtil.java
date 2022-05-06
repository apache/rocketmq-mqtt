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

package org.apache.rocketmq.mqtt.common.test.util;

import org.apache.rocketmq.mqtt.common.util.StatUtil;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_UP;

public class TestStatUtil {

    @Test
    public void test() throws Exception {

        final int keyAPv = 10, keyBPv = 20, testRt = 1000;
        // test buildKey
        String keyA = "a", keyB = "b";
        Assert.assertEquals(keyA + "," + keyB, StatUtil.buildKey(keyA, keyB));

        // test addInvoke
        StatUtil.addInvoke(keyA, testRt);
        Assert.assertEquals(1, StatUtil.nowTps(keyA));
        Assert.assertFalse(StatUtil.isOverFlow(keyA, 200));

        // test addPv
        StatUtil.addPv(keyA, keyAPv);
        Assert.assertEquals(keyAPv + 1, StatUtil.nowTps(keyA));

        // test addSecondInvoke, addSecondPV, Pv, Tps
        StatUtil.addSecondInvoke(keyB, testRt);
        Assert.assertEquals(1, StatUtil.totalPvInWindow(keyB, 60));
        StatUtil.addSecondPv(keyB, keyBPv);
        Assert.assertEquals(keyBPv + 1, StatUtil.totalPvInWindow(keyB, 60));
        Assert.assertEquals(0, StatUtil.failPvInWindow(keyB, 60));
        Assert.assertEquals(0, StatUtil.topTpsInWindow(keyA, 60));
        Assert.assertEquals(keyBPv + 1, StatUtil.topTpsInWindow(keyB, 60));

        // test RtInWindow
        Assert.assertEquals(testRt, StatUtil.maxRtInWindow(keyB, 60));
        Assert.assertEquals(0, StatUtil.minRtInWindow(keyB, 60));
        int avgRt = new BigDecimal(testRt).divide(new BigDecimal(keyBPv + 1),
                ROUND_HALF_UP).intValue();
        Assert.assertEquals(avgRt, StatUtil.avgRtInWindow(keyB, 60));
    }

}