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

package org.apache.rocketmq.mqtt.common.hook;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class HookResult {
    public static final int SUCCESS = 200;
    public static final int FAIL = -200;
    private int code;
    private int subCode;
    private String remark;
    private byte[] data;

    public HookResult(int code, int subCode, String remark, byte[] data) {
        this.code = code;
        this.subCode = subCode;
        this.remark = remark;
        this.data = data;
    }

    public HookResult(int code, String remark, byte[] data) {
        this.code = code;
        this.remark = remark;
        this.data = data;
    }

    public static CompletableFuture<HookResult> newHookResult(int code, String remark, byte[] data) {
        CompletableFuture<HookResult> result = new CompletableFuture<>();
        result.complete(new HookResult(code, remark, data));
        return result;
    }

    public static CompletableFuture<HookResult> newHookResult(int code, int subCode, String remark, byte[] data) {
        CompletableFuture<HookResult> result = new CompletableFuture<>();
        result.complete(new HookResult(code, subCode, remark, data));
        return result;
    }

    public boolean isSuccess() {
        return SUCCESS == code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getSubCode() {
        return subCode;
    }

    public void setSubCode(int subCode) {
        this.subCode = subCode;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HookResult that = (HookResult) o;
        return code == that.code && subCode == that.subCode && Objects.equals(remark, that.remark) && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(code, subCode, remark);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
