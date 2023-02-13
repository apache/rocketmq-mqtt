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

public class MetaConstants {

    public static final String CATEGORY_RETAINED_MSG = "retainedMsg";
    public static final String CATEGORY_WILL_MSG = "willMsg";
    public static final String CATEGORY_HASH_KV = "hashKv";

    public static final String NOT_FOUND = "NOT_FOUND";

    public static final String READ_INDEX_TYPE = "readIndexType";
    public static final String ANY_READ_TYPE = "anyRead";

    public static final String OP_KV_GET = "get";
    public static final String OP_KV_GET_HASH = "getHash";
    public static final String OP_HASH_KV_FIELD = "field";
    public static final String OP_KV_PUT = "put";
    public static final String OP_KV_PUT_HASH = "putHash";
    public static final String OP_KV_DEL = "del";
    public static final String OP_KV_DEL_HASH = "delHash";
}
