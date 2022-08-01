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

package org.apache.rocketmq.mqtt.common.test.hook;

import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHookResult {
    private final int code = 200;
    private final int subCode = 200;
    private final String remark = "test";

    @Test
    public void test() throws ExecutionException, InterruptedException {
        HookResult hookResult = new HookResult(code, remark, null);
        CompletableFuture<HookResult> future = HookResult.newHookResult(code, remark, null);
        assertEquals(hookResult, future.get());
        assertTrue(future.isDone());

        hookResult = new HookResult(code, subCode, remark, null);
        future = HookResult.newHookResult(code, subCode, remark, null);
        assertEquals(hookResult, future.get());
        assertTrue(future.isDone());
    }
}
