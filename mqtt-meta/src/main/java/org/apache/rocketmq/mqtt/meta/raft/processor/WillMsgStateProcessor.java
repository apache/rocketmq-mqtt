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

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.google.protobuf.ByteString;
import org.apache.rocketmq.mqtt.common.model.consistency.ReadRequest;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.common.model.consistency.WriteRequest;
import org.apache.rocketmq.mqtt.meta.raft.rpc.Constants;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public class WillMsgStateProcessor extends StateProcessor {
    private static Logger logger = LoggerFactory.getLogger(WillMsgStateProcessor.class);

    private static final String BD_PATH = System.getProperty("user.home") + "/mqtt_meta/will_db/";
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private RocksDB rocksDB;
    private WriteOptions writeOptions;
    private boolean sync = false;
    private boolean disableWAL = true;

    public WillMsgStateProcessor() {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();

        try {
            RocksDB.loadLibrary();

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(sync);
            this.writeOptions.setDisableWAL(!sync && disableWAL);

            Options options = new Options();
            Statistics statistics = new Statistics();
            statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
            options.setDbLogDir(BD_PATH).
                    setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL).
                    setCreateIfMissing(true).
                    setCreateMissingColumnFamilies(true).
                    setMaxOpenFiles(-1).
                    setMaxLogFileSize(SizeUnit.GB).
                    setKeepLogFileNum(5).
                    setMaxManifestFileSize(SizeUnit.GB).
                    setAllowConcurrentMemtableWrite(false).
                    setStatistics(statistics).
                    setMaxBackgroundJobs(32).
                    setMaxSubcompactions(4);

            rocksDB = RocksDB.open(options, BD_PATH);
        } catch (RocksDBException e) {
            logger.error("init will processor: rocksdb open error", e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Response onReadRequest(ReadRequest request) throws Exception {
        try {
            String operation = request.getOperation();
            String key = request.getKey();


            if ("get".equals(operation)) {
                return get(key.getBytes());
            } else if ("scan".equals(operation)) {
                String startKey = request.getExtDataMap().get("startKey");
                String endKey = request.getExtDataMap().get("endKey");
                return scan(startKey.getBytes(), endKey.getBytes());
            }
        } catch (Exception e) {
            if (request.getKey() == null) {
                logger.error("Fail to delete, startKey {}, endKey {}", request.getExtDataMap().get("startKey"), request.getExtDataMap().get("endKey"), e);
            } else {
                logger.error("Fail to process will WriteRequest, k {}", request.getKey(), e);
            }

            throw e;
        }
        return null;
    }

    @Override
    public Response onWriteRequest(WriteRequest log) throws Exception {
        try {
            String operation = log.getOperation();
            String key = log.getKey();
            byte[] value = log.getData().toByteArray();

            if ("put".equals(operation)) {
                return put(key.getBytes(), value);
            } else if ("delete".equals(operation)) {
                return delete(key.getBytes());
            } else if ("compareAndPut".equals(operation)) {
                return compareAndPut(key.getBytes(), log.getExtDataMap().get("expectValue").getBytes(), value);
            }
        } catch (Exception e) {
            logger.error("Fail to process will WriteRequest, k {}", log.getKey(), e);
            throw e;
        }
        return null;
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally) {

    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return false;
    }

    @Override
    public String groupCategory() {
        return Constants.WILL_MSG;
    }

    public Response put(byte[] key, byte[] value) throws RocksDBException {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.rocksDB.put(this.writeOptions, key, value);

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

    public Response delete(byte[] key) throws Exception {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            this.rocksDB.delete(this.writeOptions, key);

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

    public Response compareAndPut(byte[] key, byte[] expectValue, byte[] updateValue) throws Exception {
        final Lock writeLock = this.readWriteLock.writeLock();
        writeLock.lock();
        try {
            final byte[] actual = this.rocksDB.get(key);
            if (Arrays.equals(expectValue, actual)) {
                this.rocksDB.put(this.writeOptions, key, updateValue);
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

    public Response get(byte[] key) throws Exception {
        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {
            byte[] value = this.rocksDB.get(key);
            if (value == null) {
                value = Constants.NOT_FOUND.getBytes();
            }

            return Response.newBuilder()
                    .setSuccess(true)
                    .setData(ByteString.copyFrom(value))
                    .build();
        } catch (final Exception e) {
            logger.error("Fail to delete, k {}", key, e);
            throw e;
        } finally {
            readLock.unlock();
        }
    }

    public Response scan(byte[] startKey, byte[] endKey) throws Exception {
        Map<String, String> result = new HashMap<>();

        final Lock readLock = this.readWriteLock.readLock();
        readLock.lock();
        try {

            final RocksIterator it = this.rocksDB.newIterator();
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
