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

package org.apache.rocketmq.mqtt.cs.config;

public class WillLoopConf {
    private int maxScanNodeNum = 100;
    private int maxScanClientNum = 100000;
    private int scanNumOnce = 100;
    private boolean enableWill = true;

    public int getMaxScanNodeNum() {
        return maxScanNodeNum;
    }

    public void setMaxScanNodeNum(int maxScanNodeNum) {
        this.maxScanNodeNum = maxScanNodeNum;
    }

    public int getMaxScanClientNum() {
        return maxScanClientNum;
    }

    public void setMaxScanClientNum(int maxScanClientNum) {
        this.maxScanClientNum = maxScanClientNum;
    }

    public int getScanNumOnce() {
        return scanNumOnce;
    }

    public void setScanNumOnce(int scanNumOnce) {
        this.scanNumOnce = scanNumOnce;
    }

    public boolean isEnableWill() {
        return enableWill;
    }

    public void setEnableWill(boolean enableWill) {
        this.enableWill = enableWill;
    }
}
