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

package org.apache.rocketmq.mqtt.meta.core;

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.FollowerStateListener;
import com.alipay.sofa.jraft.rhea.LeaderStateListener;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaIterator;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.CASEntry;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Copy it from Sofajraft and wrap it as a Meta API
 */
@Service
public class MetaClient {

    @Resource
    private MetaConf metaConf;
    private RheaKVStore rheaKVStore;

    @PostConstruct
    public void init() {
        String allNodeAddress = IpUtil.convertAllNodeAddress(metaConf.getAllNodeAddress(),
                metaConf.getMetaPort());

        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured.newConfigured()
                .withInitialServerList(-1L /* default id */, allNodeAddress)
                .config();

        CliOptions cliOptions = new CliOptions();
        cliOptions.setTimeoutMs(10000);
        cliOptions.setMaxRetry(3);
        cliOptions.setRpcConnectTimeoutMs(10000);
        cliOptions.setRpcDefaultTimeout(10000);
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured()
                .withFake(true)
                .withRegionRouteTableOptionsList(regionRouteTableOptionsList)
                .withCliOptions(cliOptions)
                .config();

        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setRpcTimeoutMillis(10000);
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
                .withClusterName(metaConf.getClusterName())
                .withPlacementDriverOptions(pdOpts)
                .withRpcOptions(rpcOptions)
                .withFutureTimeoutMillis(10000)
                .config();

        this.rheaKVStore = new DefaultRheaKVStore();
        this.rheaKVStore.init(opts);
    }

    @PreDestroy
    public void shutdown() {
        if (rheaKVStore != null) {
            rheaKVStore.shutdown();
        }
    }

    /**
     * Equivalent to {@code get(key, true)}.
     */
    public CompletableFuture<byte[]> get(final byte[] key) {
        return rheaKVStore.get(key);
    }

    /**
     * @see #get(byte[])
     */
    public CompletableFuture<byte[]> get(final String key) {
        return rheaKVStore.get(key);
    }

    /**
     * Get which returns a new byte array storing the value associated
     * with the specified input key if any.  null will be returned if
     * the specified key is not found.
     *
     * @param key          the key retrieve the value.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a byte array storing the value associated with the input key if
     * any.  null if it does not find the specified key.
     */
    public CompletableFuture<byte[]> get(final byte[] key, final boolean readOnlySafe) {
        return rheaKVStore.get(key, readOnlySafe);
    }

    /**
     * @see #get(byte[], boolean)
     */
    public CompletableFuture<byte[]> get(final String key, final boolean readOnlySafe) {
        return rheaKVStore.get(key, readOnlySafe);
    }

    /**
     * @see #get(byte[])
     */
    public byte[] bGet(final byte[] key) {
        return rheaKVStore.bGet(key);
    }

    /**
     * @see #get(String)
     */
    public byte[] bGet(final String key) {
        return rheaKVStore.bGet(key);
    }

    /**
     * @see #get(byte[], boolean)
     */
    public byte[] bGet(final byte[] key, final boolean readOnlySafe) {
        return rheaKVStore.bGet(key, readOnlySafe);
    }

    /**
     * @see #get(String, boolean)
     */
    public byte[] bGet(final String key, final boolean readOnlySafe) {
        return rheaKVStore.bGet(key, readOnlySafe);
    }

    /**
     * Equivalent to {@code multiGet(keys, true)}.
     */
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys) {
        return rheaKVStore.multiGet(keys);
    }

    /**
     * Returns a map of keys for which values were found in database.
     *
     * @param keys         list of keys for which values need to be retrieved.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @return a map where key of map is the key passed by user and value for map
     * entry is the corresponding value in database.
     */
    public CompletableFuture<Map<ByteArray, byte[]>> multiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        return rheaKVStore.multiGet(keys, readOnlySafe);
    }

    /**
     * @see #multiGet(List)
     */
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys) {
        return rheaKVStore.bMultiGet(keys);
    }

    /**
     * @see #multiGet(List, boolean)
     */
    public Map<ByteArray, byte[]> bMultiGet(final List<byte[]> keys, final boolean readOnlySafe) {
        return rheaKVStore.bMultiGet(keys, readOnlySafe);
    }

    /**
     * Returns whether database contains the specified input key.
     *
     * @param key the specified key database contains.
     * @return whether database contains the specified key.
     */
    public CompletableFuture<Boolean> containsKey(final byte[] key) {
        return rheaKVStore.containsKey(key);
    }

    /**
     * @see #containsKey(byte[])
     */
    public CompletableFuture<Boolean> containsKey(final String key) {
        return rheaKVStore.containsKey(key);
    }

    /**
     * @see #containsKey(byte[])
     */
    public Boolean bContainsKey(final byte[] key) {
        return rheaKVStore.bContainsKey(key);
    }

    /**
     * @see #containsKey(byte[])
     */
    public Boolean bContainsKey(final String key) {
        return rheaKVStore.bContainsKey(key);
    }

    /**
     * Equivalent to {@code scan(startKey, endKey, true)}.
     */
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey) {
        return rheaKVStore.scan(startKey, endKey);
    }

    /**
     * @see #scan(byte[], byte[])
     */
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey) {
        return rheaKVStore.scan(startKey, endKey);
    }

    /**
     * Equivalent to {@code scan(startKey, endKey, readOnlySafe, true)}.
     */
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey,
                                                 final boolean readOnlySafe) {
        return rheaKVStore.scan(startKey, endKey, readOnlySafe);
    }

    /**
     * @see #scan(byte[], byte[], boolean)
     */
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey,
                                                 final boolean readOnlySafe) {
        return rheaKVStore.scan(startKey, endKey, readOnlySafe);
    }

    /**
     * Query all data in the key of range [startKey, endKey).
     * <p>
     * Provide consistent reading if {@code readOnlySafe} is true.
     *
     * Scanning across multi regions maybe slower and devastating.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded).
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a list where the key of range [startKey, endKey) passed by user
     * and value for {@code KVEntry}
     */
    public CompletableFuture<List<KVEntry>> scan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                                                 final boolean returnValue) {
        return rheaKVStore.scan(startKey, endKey, readOnlySafe);
    }

    /**
     * @see #scan(byte[], byte[], boolean, boolean)
     */
    public CompletableFuture<List<KVEntry>> scan(final String startKey, final String endKey, final boolean readOnlySafe,
                                                 final boolean returnValue) {
        return rheaKVStore.scan(startKey, endKey, readOnlySafe);
    }

    /**
     * @see #scan(byte[], byte[])
     */
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey) {
        return rheaKVStore.bScan(startKey, endKey);
    }

    /**
     * @see #scan(String, String)
     */
    public List<KVEntry> bScan(final String startKey, final String endKey) {
        return rheaKVStore.bScan(startKey, endKey);
    }

    /**
     * @see #scan(String, String, boolean)
     */
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe) {
        return rheaKVStore.bScan(startKey, endKey, readOnlySafe);
    }

    /**
     * @see #scan(String, String, boolean)
     */
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe) {
        return rheaKVStore.bScan(startKey, endKey, readOnlySafe);
    }

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    public List<KVEntry> bScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return rheaKVStore.bScan(startKey, endKey, readOnlySafe, returnValue);
    }

    /**
     * @see #scan(String, String, boolean, boolean)
     */
    public List<KVEntry> bScan(final String startKey, final String endKey, final boolean readOnlySafe,
                               final boolean returnValue) {
        return rheaKVStore.bScan(startKey, endKey, readOnlySafe, returnValue);
    }

    ///**
    // * Equivalent to {@code reverseScan(startKey, endKey, true)}.
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey);
    //
    ///**
    // * @see #reverseScan(byte[], byte[])
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey);
    //
    ///**
    // * Equivalent to {@code reverseScan(startKey, endKey, readOnlySafe, true)}.
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);
    //
    ///**
    // * @see #reverseScan(byte[], byte[], boolean)
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey, final boolean readOnlySafe);
    //
    ///**
    // * Reverse query all data in the key of range [startKey, endKey).
    // * <p>
    // * Provide consistent reading if {@code readOnlySafe} is true.
    // *
    // * Reverse scanning is usually much worse than forward scanning.
    // *
    // * Reverse scanning across multi regions maybe slower and devastating.
    // *
    // * @param startKey     first key to reverse scan within database (included),
    // *                     null means 'max-key' in the database.
    // * @param endKey       last key to reverse scan within database (excluded).
    // *                     null means 'min-key' in the database.
    // * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
    // *                     is true.
    // * @param returnValue  whether to return value.
    // * @return a list where the key of range [startKey, endKey) passed by user
    // * and value for {@code KVEntry}
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final byte[] startKey, final byte[] endKey,
    //    final boolean readOnlySafe, final boolean returnValue);
    //
    ///**
    // * @see #reverseScan(byte[], byte[], boolean, boolean)
    // */
    //CompletableFuture<List<KVEntry>> reverseScan(final String startKey, final String endKey,
    //    final boolean readOnlySafe, final boolean returnValue);
    //
    ///**
    // * @see #reverseScan(byte[], byte[])
    // */
    //List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey);
    //
    ///**
    // * @see #scan(String, String)
    // */
    //List<KVEntry> bReverseScan(final String startKey, final String endKey);
    //
    ///**
    // * @see #scan(String, String, boolean)
    // */
    //List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe);
    //
    ///**
    // * @see #scan(String, String, boolean)
    // */
    //List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe);
    //
    ///**
    // * @see #reverseScan(String, String, boolean, boolean)
    // */
    //List<KVEntry> bReverseScan(final byte[] startKey, final byte[] endKey, final boolean readOnlySafe,
    //    final boolean returnValue);
    //
    ///**
    // * @see #reverseScan(String, String, boolean, boolean)
    // */
    //List<KVEntry> bReverseScan(final String startKey, final String endKey, final boolean readOnlySafe,
    //    final boolean returnValue);

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true)}.
     */
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize) {
        return rheaKVStore.iterator(startKey, endKey, bufSize);
    }

    /**
     * @see #iterator(byte[], byte[], int)
     */
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize) {
        return rheaKVStore.iterator(startKey, endKey, bufSize);
    }

    /**
     * Equivalent to {@code iterator(startKey, endKey, bufSize, true, true)}.
     */
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return rheaKVStore.iterator(startKey, endKey, bufSize, readOnlySafe);
    }

    /**
     * @see #iterator(byte[], byte[], int, boolean)
     */
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe) {
        return rheaKVStore.iterator(startKey, endKey, bufSize, readOnlySafe);
    }

    /**
     * Returns a remote iterator over the contents of the database.
     *
     * Functionally similar to {@link #scan(byte[], byte[], boolean)},
     * but iterator only returns a small amount of data at a time, avoiding
     * a large amount of data returning to the client at one time causing
     * memory overflow, can think of it as a 'lazy scan' method.
     *
     * @param startKey     first key to scan within database (included),
     *                     null means 'min-key' in the database.
     * @param endKey       last key to scan within database (excluded),
     *                     null means 'max-key' in the database.
     * @param readOnlySafe provide consistent reading if {@code readOnlySafe}
     *                     is true.
     * @param returnValue  whether to return value.
     * @return a iterator where the key of range [startKey, endKey) passed by
     * user and value for {@code KVEntry}
     */
    public RheaIterator<KVEntry> iterator(final byte[] startKey, final byte[] endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return rheaKVStore.iterator(startKey, endKey, bufSize, readOnlySafe, returnValue);
    }

    /**
     * @see #iterator(byte[], byte[], int, boolean, boolean)
     */
    public RheaIterator<KVEntry> iterator(final String startKey, final String endKey, final int bufSize,
                                          final boolean readOnlySafe, final boolean returnValue) {
        return rheaKVStore.iterator(startKey, endKey, bufSize, readOnlySafe, returnValue);
    }

    /**
     * Get a globally unique auto-increment sequence.
     *
     * Be careful do not to try to get or update the value of {@code seqKey}
     * by other methods, you won't get it.
     *
     * @param seqKey the key of sequence
     * @param step   number of values obtained
     * @return a values range of [startValue, endValue)
     */
    public CompletableFuture<Sequence> getSequence(final byte[] seqKey, final int step) {
        return rheaKVStore.getSequence(seqKey, step);
    }

    /**
     * @see #getSequence(byte[], int)
     */
    public CompletableFuture<Sequence> getSequence(final String seqKey, final int step) {
        return rheaKVStore.getSequence(seqKey, step);
    }

    /**
     * @see #getSequence(byte[], int)
     */
    public Sequence bGetSequence(final byte[] seqKey, final int step) {
        return rheaKVStore.bGetSequence(seqKey, step);
    }

    /**
     * @see #getSequence(byte[], int)
     */
    public Sequence bGetSequence(final String seqKey, final int step) {
        return rheaKVStore.bGetSequence(seqKey, step);
    }

    /**
     * Gets the latest sequence start value, this is a read-only operation.
     *
     * Equivalent to {@code getSequence(seqKey, 0)}.
     *
     * @see #getSequence(byte[], int)
     *
     * @param seqKey the key of sequence
     * @return the latest sequence value
     */
    public CompletableFuture<Long> getLatestSequence(final byte[] seqKey) {
        return rheaKVStore.getLatestSequence(seqKey);
    }

    /**
     * @see #getLatestSequence(byte[])
     */
    public CompletableFuture<Long> getLatestSequence(final String seqKey) {
        return rheaKVStore.getLatestSequence(seqKey);
    }

    /**
     * @see #getLatestSequence(byte[])
     */
    public Long bGetLatestSequence(final byte[] seqKey) {
        return rheaKVStore.bGetLatestSequence(seqKey);
    }

    /**
     * @see #getLatestSequence(byte[])
     */
    public Long bGetLatestSequence(final String seqKey) {
        return rheaKVStore.bGetLatestSequence(seqKey);
    }

    /**
     * Reset the sequence to 0.
     *
     * @param seqKey the key of sequence
     */
    public CompletableFuture<Boolean> resetSequence(final byte[] seqKey) {
        return rheaKVStore.resetSequence(seqKey);
    }

    /**
     * @see #resetSequence(byte[])
     */
    public CompletableFuture<Boolean> resetSequence(final String seqKey) {
        return rheaKVStore.resetSequence(seqKey);
    }

    /**
     * @see #resetSequence(byte[])
     */
    public Boolean bResetSequence(final byte[] seqKey) {
        return rheaKVStore.bResetSequence(seqKey);
    }

    /**
     * @see #resetSequence(byte[])
     */
    public Boolean bResetSequence(final String seqKey) {
        return rheaKVStore.bResetSequence(seqKey);
    }

    /**
     * Set the database entry for "key" to "value".
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return {@code true} if success.
     */
    public CompletableFuture<Boolean> put(final byte[] key, final byte[] value) {
        return rheaKVStore.put(key, value);
    }

    /**
     * @see #put(byte[], byte[])
     */
    public CompletableFuture<Boolean> put(final String key, final byte[] value) {
        return rheaKVStore.put(key, value);
    }

    /**
     * @see #put(byte[], byte[])
     */
    public Boolean bPut(final byte[] key, final byte[] value) {
        return rheaKVStore.bPut(key, value);
    }

    /**
     * @see #put(byte[], byte[])
     */
    public Boolean bPut(final String key, final byte[] value) {
        return rheaKVStore.bPut(key, value);
    }

    /**
     * Set the database entry for "key" to "value", and return the
     * previous value associated with "key", or null if there was no
     * mapping for "key".
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with "key", or null if
     * there was no mapping for "key".
     */
    public CompletableFuture<byte[]> getAndPut(final byte[] key, final byte[] value) {
        return rheaKVStore.getAndPut(key, value);
    }

    /**
     * @see #getAndPut(byte[], byte[])
     */
    public CompletableFuture<byte[]> getAndPut(final String key, final byte[] value) {
        return rheaKVStore.getAndPut(key, value);
    }

    /**
     * @see #getAndPut(byte[], byte[])
     */
    public byte[] bGetAndPut(final byte[] key, final byte[] value) {
        return rheaKVStore.bGetAndPut(key, value);
    }

    /**
     * @see #getAndPut(byte[], byte[])
     */
    public byte[] bGetAndPut(final String key, final byte[] value) {
        return rheaKVStore.bGetAndPut(key, value);
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value equal (compare bytes) the expected value.
     *
     * @param key    the key retrieve the value
     * @param expect the expected value
     * @param update the new value
     * @return true if successful. False return indicates that the actual
     * value was not equal to the expected value.
     */
    public CompletableFuture<Boolean> compareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        return rheaKVStore.compareAndPut(key, expect, update);
    }

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    public CompletableFuture<Boolean> compareAndPut(final String key, final byte[] expect, final byte[] update) {
        return rheaKVStore.compareAndPut(key, expect, update);
    }

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    public Boolean bCompareAndPut(final byte[] key, final byte[] expect, final byte[] update) {
        return rheaKVStore.bCompareAndPut(key, expect, update);
    }

    /**
     * @see #compareAndPut(byte[], byte[], byte[])
     */
    public Boolean bCompareAndPut(final String key, final byte[] expect, final byte[] update) {
        return rheaKVStore.bCompareAndPut(key, expect, update);
    }

    /**
     * Add merge operand for key/value pair.
     *
     * <pre>
     *     // Writing aa under key
     *     db.put("key", "aa");
     *
     *     // Writing bb under key
     *     db.merge("key", "bb");
     *
     *     assertThat(db.get("key")).isEqualTo("aa,bb");
     * </pre>
     *
     * @param key   the specified key to be merged.
     * @param value the value to be merged with the current value for
     *              the specified key.
     * @return {@code true} if success.
     */
    public CompletableFuture<Boolean> merge(final String key, final String value) {
        return rheaKVStore.merge(key, value);
    }

    /**
     * @see #merge(String, String)
     */
    public Boolean bMerge(final String key, final String value) {
        return rheaKVStore.bMerge(key, value);
    }

    /**
     * The batch method of {@link #put(byte[], byte[])}
     */
    public CompletableFuture<Boolean> put(final List<KVEntry> entries) {
        return rheaKVStore.put(entries);
    }

    /**
     * @see #put(List)
     */
    public Boolean bPut(final List<KVEntry> entries) {
        return rheaKVStore.bPut(entries);
    }

    /**
     * The batch method of {@link #compareAndPut(byte[], byte[], byte[])}
     */
    public CompletableFuture<Boolean> compareAndPutAll(final List<CASEntry> entries) {
        return rheaKVStore.compareAndPutAll(entries);

    }

    /**
     * @see #compareAndPutAll(List)
     */
    public Boolean bCompareAndPutAll(final List<CASEntry> entries) {
        return rheaKVStore.bCompareAndPutAll(entries);
    }

    /**
     * If the specified key is not already associated with a value
     * associates it with the given value and returns {@code null},
     * else returns the current value.
     *
     * @param key   the specified key to be inserted.
     * @param value the value associated with the specified key.
     * @return the previous value associated with the specified key,
     * or {@code null} if there was no mapping for the key.
     * (A {@code null} return can also indicate that the database.
     * previously associated {@code null} with the key.
     */
    public CompletableFuture<byte[]> putIfAbsent(final byte[] key, final byte[] value) {
        return rheaKVStore.putIfAbsent(key, value);
    }

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    public CompletableFuture<byte[]> putIfAbsent(final String key, final byte[] value) {
        return rheaKVStore.putIfAbsent(key, value);
    }

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    public byte[] bPutIfAbsent(final byte[] key, final byte[] value) {
        return rheaKVStore.bPutIfAbsent(key, value);
    }

    /**
     * @see #putIfAbsent(byte[], byte[])
     */
    public byte[] bPutIfAbsent(final String key, final byte[] value) {
        return rheaKVStore.bPutIfAbsent(key, value);
    }

    /**
     * Delete the database entry (if any) for "key".
     *
     * @param key key to delete within database.
     * @return {@code true} if success.
     */
    public CompletableFuture<Boolean> delete(final byte[] key) {
        return rheaKVStore.delete(key);
    }

    /**
     * @see #delete(byte[])
     */
    public CompletableFuture<Boolean> delete(final String key) {
        return rheaKVStore.delete(key);
    }

    /**
     * @see #delete(byte[])
     */
    public Boolean bDelete(final byte[] key) {
        return rheaKVStore.bDelete(key);
    }

    /**
     * @see #delete(byte[])
     */
    public Boolean bDelete(final String key) {
        return rheaKVStore.bDelete(key);
    }

    /**
     * Removes the database entries in the range ["startKey", "endKey"), i.e.,
     * including "startKey" and excluding "endKey".
     *
     * @param startKey first key to delete within database (included)
     * @param endKey   last key to delete within database (excluded)
     * @return {@code true} if success.
     */
    public CompletableFuture<Boolean> deleteRange(final byte[] startKey, final byte[] endKey) {
        return rheaKVStore.deleteRange(startKey, endKey);
    }

    /**
     * @see #deleteRange(byte[], byte[])
     */
    public CompletableFuture<Boolean> deleteRange(final String startKey, final String endKey) {
        return rheaKVStore.deleteRange(startKey, endKey);
    }

    /**
     * @see #deleteRange(byte[], byte[])
     */
    public Boolean bDeleteRange(final byte[] startKey, final byte[] endKey) {
        return rheaKVStore.bDeleteRange(startKey, endKey);
    }

    /**
     * @see #deleteRange(byte[], byte[])
     */
    public Boolean bDeleteRange(final String startKey, final String endKey) {
        return rheaKVStore.bDeleteRange(startKey, endKey);
    }

    /**
     * The batch method of {@link #delete(byte[])}
     */
    public CompletableFuture<Boolean> delete(final List<byte[]> keys) {
        return rheaKVStore.delete(keys);
    }

    /**
     * @see #delete(List)
     */
    public Boolean bDelete(final List<byte[]> keys) {
        return rheaKVStore.bDelete(keys);
    }

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit) {
        return rheaKVStore.getDistributedLock(target, lease, unit);
    }

    /**
     * @see #getDistributedLock(String, long, TimeUnit, ScheduledExecutorService)
     */
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit) {
        return rheaKVStore.getDistributedLock(target, lease, unit);
    }

    /**
     * Creates a distributed lock implementation that provides
     * exclusive access to a shared resource.
     * <p>
     * <pre>
     *      DistributedLock<byte[]> lock = ...;
     *      if (lock.tryLock()) {
     *          try {
     *              // manipulate protected state
     *          } finally {
     *              lock.unlock();
     *          }
     *      } else {
     *          // perform alternative actions
     *      }
     * </pre>
     *
     * The algorithm relies on the assumption that while there is no
     * synchronized clock across the processes, still the local time in
     * every process flows approximately at the same rate, with an error
     * which is small compared to the auto-release time of the lock.
     *
     * @param target   key of the distributed lock that acquired.
     * @param lease    the lease time for the distributed lock to live.
     * @param unit     the time unit of the {@code expire} argument.
     * @param watchdog if the watchdog is not null, it will auto keep
     *                 lease of current lock, otherwise won't keep lease,
     *                 this method dose not pay attention to the life cycle
     *                 of watchdog, please maintain it yourself.
     * @return a distributed lock instance.
     */
    public DistributedLock<byte[]> getDistributedLock(final byte[] target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return rheaKVStore.getDistributedLock(target, lease, unit, watchdog);
    }

    /**
     * @see #getDistributedLock(byte[], long, TimeUnit, ScheduledExecutorService)
     */
    public DistributedLock<byte[]> getDistributedLock(final String target, final long lease, final TimeUnit unit,
                                                      final ScheduledExecutorService watchdog) {
        return rheaKVStore.getDistributedLock(target, lease, unit, watchdog);
    }

    /**
     * Returns current placement driver client instance.
     */
    public PlacementDriverClient getPlacementDriverClient() {
        return rheaKVStore.getPlacementDriverClient();
    }

    /**
     * Add a listener for the state change of the leader with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    public void addLeaderStateListener(final long regionId, final LeaderStateListener listener) {
        rheaKVStore.addLeaderStateListener(regionId, listener);
    }

    /**
     * Add a listener for the state change of the follower with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    public void addFollowerStateListener(final long regionId, final FollowerStateListener listener) {
        rheaKVStore.addFollowerStateListener(regionId, listener);
    }

    /**
     * Add a listener for the state change (leader, follower) with
     * this region.
     * <p>
     * In a special case, if that is a single region environment,
     * then regionId = -1 as the input parameter.
     */
    public void addStateListener(final long regionId, final StateListener listener) {
        rheaKVStore.addStateListener(regionId, listener);
    }
}
