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

package org.apache.rocketmq.mqtt.meta.rocksdb;

import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.google.common.collect.Lists;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RocksDBEngine {
    private RocksDB rdb;
    private String rdbPath;
    private DBOptions dbOptions;
    private ColumnFamilyHandle defaultHandle;
    private final List<ColumnFamilyOptions> cfOptionsList = Lists.newArrayList();
    private final List<ColumnFamilyDescriptor> cfDescriptors = Lists.newArrayList();
    private WriteOptions writeOptions;
    private RocksDBSnapshot rocksDBSnapshot;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    static {
        RocksDB.loadLibrary();
    }

    public RocksDB getRdb() {
        return rdb;
    }

    public String getRdbPath() {
        return rdbPath;
    }

    public ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public RocksDBEngine(String dbPath) {
        this.rdbPath = dbPath;
        this.rocksDBSnapshot = new RocksDBSnapshot(this);
    }

    public RocksDBSnapshot getRocksDBSnapshot() {
        return rocksDBSnapshot;
    }

    public void init() throws RocksDBException {
        this.dbOptions = createDBOptions();
        final ColumnFamilyOptions cfOptions = createColumnFamilyOptions();
        this.cfOptionsList.add(cfOptions);
        // default column family
        this.cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);
        // If `sync` is true, `disableWAL` must be set false.
        this.writeOptions.setDisableWAL(true);
        // Delete existing data, relying on raft's snapshot and log playback
        // to reply to the data is the correct behavior.
        destroyRocksDB();
        openRocksDB();
    }

    public DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksDBEngine.class).setEnv(Env.getDefault());
    }

    public ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory.getRocksDBTableFormatConfig(RocksDBEngine.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RocksDBEngine.class)
                .setTableFormatConfig(tConfig)
                .setMergeOperator(new StringAppendOperator());
    }

    protected void destroyRocksDB() throws RocksDBException {
        try (final Options opt = new Options()) {
            RocksDB.destroyDB(rdbPath, opt);
        }
    }

    protected void openRocksDB() throws RocksDBException {
        final List<ColumnFamilyHandle> cfHandles = Lists.newArrayList();
        this.rdb = RocksDB.open(this.dbOptions, rdbPath, this.cfDescriptors, cfHandles);
        this.defaultHandle = cfHandles.get(0);
    }

    protected void closeRocksDB() {
        if (this.rdb != null) {
            this.rdb.close();
            this.rdb = null;
        }
    }

}
