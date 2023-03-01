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

    public static final String RETAIN_REQ_READ_PARAM_TOPIC = "topic";
    public static final String RETAIN_REQ_READ_PARAM_FIRST_TOPIC = "firstTopic";
    public static final String RETAIN_REQ_READ_PARAM_OPERATION_TRIE = "trie";
    public static final String RETAIN_REQ_READ_PARAM_OPERATION_TOPIC = "topic";

    public static final String RETAIN_REQ_WRITE_PARAM_FIRST_TOPIC = "firstTopic";
    public static final String RETAIN_REQ_WRITE_PARAM_TOPIC = "topic";
    public static final String RETAIN_REQ_WRITE_PARAM_IS_EMPTY = "isEmpty";
    public static final String RETAIN_REQ_WRITE_PARAM_EXPIRE = "expire";

    public static final String WILL_REQ_READ_GET = "get";
    public static final String WILL_REQ_READ_SCAN = "scan";
    public static final String WILL_REQ_READ_START_KEY = "startKey";
    public static final String WILL_REQ_READ_END_KEY = "endKey";

    public static final String WILL_REQ_WRITE_PUT = "put";
    public static final String WILL_REQ_WRITE_DELETE = "delete";

    public static final String WILL_REQ_WRITE_COMPARE_AND_PUT = "compareAndPut";
    public static final String WILL_REQ_WRITE_EXPECT_VALUE = "expectValue";


}
