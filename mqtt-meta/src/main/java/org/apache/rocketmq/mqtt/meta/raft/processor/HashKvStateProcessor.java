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

package org.apache.rocketmq.mqtt.meta.raft.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.MqttRaftServer;
import org.apache.rocketmq.mqtt.meta.raft.MqttStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.CATEGORY_HASH_KV;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_HASH_KV_FIELD;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_DEL;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_DEL_HASH;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_GET;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_GET_HASH;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_PUT;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.OP_KV_PUT_HASH;

public class HashKvStateProcessor extends StateProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashKvStateProcessor.class);
    private MqttRaftServer server;

    public HashKvStateProcessor(MqttRaftServer server) {
        this.server = server;
    }

    @Override
    public Response onReadRequest(ReadRequest request) throws Exception {
        try {
            MqttStateMachine sm = server.getMqttStateMachine(request.getGroup());
            if (sm == null) {
                LOGGER.error("Fail to process will ReadRequest , Not Found SM for {}", request.getGroup());
                return null;
            }
            String operation = request.getOperation();
            String key = request.getKey();
            if (OP_KV_GET.equals(operation)) {
                return get(sm.getRocksDBEngine(), key.getBytes(StandardCharsets.UTF_8));
            } else if (OP_KV_GET_HASH.equals(operation)) {
                byte[] value = getRdb(sm.getRocksDBEngine(), key.getBytes(StandardCharsets.UTF_8));
                if (value == null) {
                    return Response.newBuilder()
                            .setSuccess(true)
                            .build();
                }
                Map<String, String> map = JSONObject.parseObject(
                        new String(value, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {
                        });
                String field = request.getExtDataMap().get(OP_HASH_KV_FIELD);
                Map<String, String> result = new HashMap<>();
                if (StringUtils.isNotBlank(field)) {
                    result.put(field, map.get(field));
                } else {
                    result.putAll(map);
                }
                return Response.newBuilder()
                        .setSuccess(true)
                        .putAllDataMap(result)
                        .build();
            }
        } catch (Exception e) {
            LOGGER.error("Fail to process will ReadRequest, k {}", request.getKey(), e);
            throw e;
        }
        return null;
    }

    @Override
    public Response onWriteRequest(WriteRequest log) throws Exception {
        try {
            MqttStateMachine sm = server.getMqttStateMachine(log.getGroup());
            if (sm == null) {
                LOGGER.error("Fail to process will WriteRequest , Not Found SM for {}", log.getGroup());
                return null;
            }
            String operation = log.getOperation();
            String key = log.getKey();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] value = log.getData().toByteArray();
            if (value == null) {
                return null;
            }
            if (OP_KV_PUT.equals(operation)) {
                return put(sm.getRocksDBEngine(), keyBytes, value);
            } else if (OP_KV_DEL.equals(operation)) {
                return delete(sm.getRocksDBEngine(), keyBytes);
            } else if (OP_KV_PUT_HASH.equals(operation)) {
                Map<String, String> map = JSONObject.parseObject(
                        new String(value, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {
                        });
                byte[] oldValue = getRdb(sm.getRocksDBEngine(), keyBytes);
                if (oldValue != null && oldValue.length > 1) {
                    Map<String, String> oldMap = JSONObject.parseObject(
                            new String(oldValue, StandardCharsets.UTF_8),
                            new TypeReference<Map<String, String>>() {
                            });
                    oldMap.putAll(map);
                    map.putAll(oldMap);
                }
                return put(sm.getRocksDBEngine(), keyBytes, JSON.toJSONBytes(map));
            } else if (OP_KV_DEL_HASH.equals(operation)) {
                String field = log.getExtDataMap().get(OP_HASH_KV_FIELD);
                if (StringUtils.isBlank(field)) {
                    return Response.newBuilder()
                            .setSuccess(false)
                            .setErrMsg("No Found Field")
                            .build();
                }
                byte[] oldValue = getRdb(sm.getRocksDBEngine(), keyBytes);
                if (oldValue == null || oldValue.length < 1) {
                    return Response.newBuilder()
                            .setSuccess(true)
                            .build();
                }
                Map<String, String> oldMap = JSONObject.parseObject(
                        new String(oldValue, StandardCharsets.UTF_8),
                        new TypeReference<Map<String, String>>() {
                        });
                if (!oldMap.containsKey(field)) {
                    return Response.newBuilder()
                            .setSuccess(true)
                            .build();
                }
                oldMap.remove(field);
                if (oldMap.isEmpty()) {
                    return delete(sm.getRocksDBEngine(), keyBytes);
                } else {
                    return put(sm.getRocksDBEngine(), keyBytes, JSON.toJSONBytes(oldMap));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Fail to process will WriteRequest, k {}", log.getKey(), e);
            throw e;
        }
        return null;
    }

    @Override
    public String groupCategory() {
        return CATEGORY_HASH_KV;
    }
}
