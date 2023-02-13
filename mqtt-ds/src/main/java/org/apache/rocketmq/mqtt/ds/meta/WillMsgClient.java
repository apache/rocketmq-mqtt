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

package org.apache.rocketmq.mqtt.ds.meta;

import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.CATEGORY_WILL_MSG;
import static org.apache.rocketmq.mqtt.common.meta.MetaConstants.READ_INDEX_TYPE;
import static org.apache.rocketmq.mqtt.common.meta.RaftUtil.WILL_RAFT_GROUP_INDEX;


@Service
public class WillMsgClient {

    private static Logger logger = LoggerFactory.getLogger(WillMsgClient.class);

    @Resource
    private MetaRpcClient metaRpcClient;

    public void put(final String key, final String value, CompletableFuture<Boolean> future) throws Exception {
        String groupId = whichGroup();
        final WriteRequest request = WriteRequest.newBuilder().
                setGroup(groupId).
                setKey(key).
                setData(ByteString.copyFrom(value.getBytes())).
                setOperation("put").
                setCategory(CATEGORY_WILL_MSG).
                build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, (result, err) -> {
            if (err == null) {
                Response rsp = (Response) result;
                if (!rsp.getSuccess()) {
                    logger.info("put kv failed. k:{} , v:{}, {}", key, value, rsp.getErrMsg());
                    future.complete(false);
                    return;
                }
                logger.debug("put kv success. k:{} , v:{}", key, value);
                future.complete(true);
            } else {
                logger.error("put kv failed. k:{} , v:{}", key, value, err);
                future.completeExceptionally(err);
            }
        }, 5000);
    }

    public void delete(final String key, CompletableFuture<Boolean> future) throws Exception {
        String groupId = whichGroup();
        final WriteRequest request = WriteRequest.newBuilder().
                setGroup(groupId).
                setKey(key).
                setOperation("delete").
                setCategory(CATEGORY_WILL_MSG).
                build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, (result, err) -> {
            if (err == null) {
                Response rsp = (Response) result;
                if (!rsp.getSuccess()) {
                    logger.info("delete kv failed. k:{} ,{}", key, rsp.getErrMsg());
                    future.complete(false);
                    return;
                }
                logger.debug("delete kv success. k:{}", key);
                future.complete(true);
            } else {
                logger.error("delete kv failed. k:{}", key, err);
                future.completeExceptionally(err);
            }
        }, 5000);
    }

    public void get(final String key, CompletableFuture<byte[]> future) throws Exception {
        String groupId = whichGroup();
        final ReadRequest request = ReadRequest.newBuilder().
                setGroup(groupId).
                setKey(key).
                setOperation("get").
                setType(READ_INDEX_TYPE).
                setCategory(CATEGORY_WILL_MSG).
                build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, (result, err) -> {
            if (err == null) {
                Response rsp = (Response) result;
                if (!rsp.getSuccess()) {
                    logger.info("get value failed. k:{}, {}", key, rsp.getErrMsg());
                    future.complete(null);
                    return;
                }
                future.complete(rsp.getData().toByteArray());
            } else {
                logger.error("get value failed. k:{}", key, err);
                future.completeExceptionally(err);
            }
        }, 5000);
    }

    public void compareAndPut(final String key, final String expectValue, final String updateValue, CompletableFuture<Boolean> future) throws Exception {
        String groupId = whichGroup();
        final WriteRequest request = WriteRequest.newBuilder().
                setGroup(groupId).
                setKey(key).
                setData(ByteString.copyFrom(updateValue.getBytes())).
                setOperation("compareAndPut").
                putExtData("expectValue", expectValue).
                setCategory(CATEGORY_WILL_MSG).
                build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, (result, err) -> {
            if (err == null) {
                Response rsp = (Response) result;
                if (!rsp.getSuccess()) {
                    logger.info("compareAndPut kv failed. k:{} , v:{}, {}", key, updateValue, rsp.getErrMsg());
                    future.complete(false);
                    return;
                }
                logger.debug("compareAndPut kv success. k:{} , v:{}", key, updateValue);
                future.complete(true);
            } else {
                logger.error("compareAndPut kv failed. k:{} , v:{}", key, updateValue, err);
                future.completeExceptionally(err);
            }
        }, 5000);
    }

    public void scan(final String startKey, final String endKey, CompletableFuture<Map<String, String>> future) throws Exception {
        String groupId = whichGroup();
        final ReadRequest request = ReadRequest.newBuilder().
                setGroup(groupId).
                setOperation("scan").
                putExtData("startKey", startKey).
                putExtData("endKey", endKey).
                setType(READ_INDEX_TYPE).
                setCategory(CATEGORY_WILL_MSG).
                build();

        metaRpcClient.getCliClientService().getRpcClient().invokeAsync(metaRpcClient.getLeader(groupId).getEndpoint(), request, (result, err) -> {
            if (err == null) {
                Response rsp = (Response) result;
                if (!rsp.getSuccess()) {
                    logger.info("scan failed. startKey:{}, endKey:{}, {}", startKey, endKey, rsp.getErrMsg());
                    future.complete(null);
                    return;
                }

                Map<String, String> res = rsp.getDataMapMap();
                future.complete(res);
            } else {
                logger.error("scan failed. startKey:{}, endKey:{}", startKey, endKey, err);
                future.completeExceptionally(err);
            }
        }, 5000);
    }

    private String whichGroup() {
        return metaRpcClient.getRaftGroups()[WILL_RAFT_GROUP_INDEX];
    }

}
