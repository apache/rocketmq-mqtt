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

import com.alipay.sofa.jraft.util.BytesUtil;
import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.common.meta.MetaConstants;
import org.apache.rocketmq.mqtt.meta.rocksdb.RocksDBEngine;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * A concrete processing class for a business state machine
 */
public abstract class StateProcessor {
    protected static Logger logger = LoggerFactory.getLogger(StateProcessor.class);

    /**
     * Process the read request to apply the state machine
     *
     * @param request
     * @return
     */
    public abstract Response onReadRequest(ReadRequest request) throws Exception;

    /**
     * Process the write request to apply the state machine
     *
     * @param log
     * @return
     */
    public abstract Response onWriteRequest(WriteRequest log) throws Exception;


    /**
     * Raft Grouping category. The grouping category and sequence number identify the unique RAFT group
     *
     * @return
     */
    public abstract String groupCategory();

    public Response get(RocksDBEngine rocksDBEngine, byte[] key) throws Exception {
        final Lock readLock = rocksDBEngine.getReadWriteLock().readLock();
        readLock.lock();
        try {
            byte[] value = rocksDBEngine.getRdb().get(key);
            if (value == null) {
                value = MetaConstants.NOT_FOUND.getBytes();
            }
            return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(value))
                    .build();
        } catch (final Exception e) {
            logger.error("Fail to get, k {}", key, e);
            throw e;
        } finally {
            readLock.unlock();
        }
    }

    public byte[] getRdb(RocksDBEngine rocksDBEngine, byte[] key) throws RocksDBException {
        final Lock readLock = rocksDBEngine.getReadWriteLock().readLock();
        readLock.lock();
        try {
            byte[] value = rocksDBEngine.getRdb().get(key);
            return value;
        } catch (final Exception e) {
            logger.error("Fail to get, k {}", key, e);
            throw e;
        } finally {
            readLock.unlock();
        }
    }

    public Response put(RocksDBEngine rocksDBEngine, byte[] key, byte[] value) throws RocksDBException {
        final Lock writeLock = rocksDBEngine.getReadWriteLock().writeLock();
        writeLock.lock();
        try {
            rocksDBEngine.getRdb().put(rocksDBEngine.getWriteOptions(), key, value);
            return Response.newBuilder()
                    .setSuccess(true)
                    .build();
        } catch (final Exception e) {
            logger.error("Fail to put, k {}, v {}", key, value, e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    public Response delete(RocksDBEngine rocksDBEngine, byte[] key) throws Exception {
        final Lock writeLock = rocksDBEngine.getReadWriteLock().writeLock();
        writeLock.lock();
        try {
            rocksDBEngine.getRdb().delete(rocksDBEngine.getWriteOptions(), key);
            return Response.newBuilder()
                    .setSuccess(true)
                    .build();
        } catch (final Exception e) {
            logger.error("Fail to delete, k {}", key, e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    public Response compareAndPut(RocksDBEngine rocksDBEngine, byte[] key, byte[] expectValue, byte[] updateValue) throws Exception {
        final Lock writeLock = rocksDBEngine.getReadWriteLock().writeLock();
        writeLock.lock();
        try {
            final byte[] actual = rocksDBEngine.getRdb().get(key);
            if (Arrays.equals(expectValue, actual)) {
                rocksDBEngine.getRdb().put(rocksDBEngine.getWriteOptions(), key, updateValue);
                return Response.newBuilder()
                        .setSuccess(true)
                        .build();
            } else {
                return Response.newBuilder()
                        .setSuccess(false)
                        .build();
            }
        } catch (final Exception e) {
            logger.error("Fail to delete, k {}", key, e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    public Response scan(RocksDBEngine rocksDBEngine, byte[] startKey, byte[] endKey, long scanNum) throws Exception {
        Map<String, String> result = new HashMap<>();
        final Lock readLock = rocksDBEngine.getReadWriteLock().readLock();
        readLock.lock();
        try {
            final RocksIterator it = rocksDBEngine.getRdb().newIterator();
            if (startKey == null) {
                it.seekToFirst();
            } else {
                it.seek(startKey);
            }
            while (it.isValid()) {
                final byte[] key = it.key();
                if (endKey != null && BytesUtil.compare(key, endKey) >= 0) {
                    break;
                }
                result.put(new String(key), new String(it.value()));
                if (result.size() >= scanNum) {
                    break;
                }
                it.next();
            }
            return Response.newBuilder()
                    .setSuccess(true)
                    .putAllDataMap(result)
                    .build();
        } catch (final Exception e) {
            logger.error("Fail to delete, startKey {}, endKey {}", startKey, endKey, e);
            throw e;
        } finally {
            readLock.unlock();
        }
    }

}
