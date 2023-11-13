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

package org.apache.rocketmq.mqtt.common.meta;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;

public class RaftUtil {
    public static final String RAFT_GROUP_NAME_PREFIX = "RAFT_GROUP_";
    public static final int RAFT_GROUP_NUM = 3;
    public static final String[] RAFT_GROUPS;

    public static final int RETAIN_RAFT_GROUP_INDEX = 0;
    public static final int WILL_RAFT_GROUP_INDEX = 1;
    public static final int HASH_KV_RAFT_GROUP_INDEX = 2;

    static {
        RAFT_GROUPS = new String[RAFT_GROUP_NUM];
        for (int i = 0; i < RAFT_GROUP_NUM; i++) {
            RAFT_GROUPS[i] = RAFT_GROUP_NAME_PREFIX + i;
        }
    }

    public static String RAFT_BASE_DIR(String group) {
        String metaBaseDir = SystemUtils.USER_HOME;
        if (System.getenv("META_BASE_DIR") != null) {
            metaBaseDir = System.getenv("META_BASE_DIR");
        }
        return metaBaseDir + File.separator + "raft" + File.separator + group;
    }

    public static String[] LIST_RAFT_GROUPS() {
        return RAFT_GROUPS;
    }
}
