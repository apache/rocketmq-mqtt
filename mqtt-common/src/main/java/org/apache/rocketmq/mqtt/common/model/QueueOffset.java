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

package org.apache.rocketmq.mqtt.common.model;

import java.util.Objects;


public class QueueOffset {
    private volatile long offset = Long.MAX_VALUE;
    private volatile byte initializingStatus = -1;

    public QueueOffset() {
    }

    public QueueOffset(long offset) {
        this.offset = offset;
    }

    public boolean isInitialized() {
        return offset != Long.MAX_VALUE || initializingStatus == 1;
    }

    public boolean isInitializing() {
        return initializingStatus == 0;
    }

    public void setInitialized() {
        initializingStatus = 1;
    }

    public void setInitializing() {
        initializingStatus = 0;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueueOffset that = (QueueOffset) o;
        return offset == that.offset && initializingStatus == that.initializingStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, initializingStatus);
    }
}
