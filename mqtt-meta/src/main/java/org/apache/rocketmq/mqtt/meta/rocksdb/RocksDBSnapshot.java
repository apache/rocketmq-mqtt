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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.zip.ZipStrategyManager;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.zip.Checksum;

public class RocksDBSnapshot {
    private static Logger logger = LoggerFactory.getLogger(RocksDBSnapshot.class);
    private static final String SNAPSHOT_DIR = "sd";
    private static final String SNAPSHOT_ARCHIVE = "sd.zip";
    private final RocksDBEngine rocksDBEngine;

    public RocksDBSnapshot(RocksDBEngine rocksDBEngine) {
        this.rocksDBEngine = rocksDBEngine;
    }

    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        final String writerPath = writer.getPath();
        final String snapshotPath = Paths.get(writerPath, SNAPSHOT_DIR).toString();
        writeSnapshot(snapshotPath);
        compressSnapshot(writer, writeMetadata(null), done);
    }

    public boolean onSnapshotLoad(final SnapshotReader reader) {
        final LocalFileMetaOutter.LocalFileMeta meta = (LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        final String readerPath = reader.getPath();
        if (meta == null) {
            logger.error("Can't find rdb snapshot file, path={}.", readerPath);
            return false;
        }
        final String snapshotPath = Paths.get(readerPath, SNAPSHOT_DIR).toString();
        try {
            decompressSnapshot(readerPath, meta);
            readSnapshot(snapshotPath);
            final File tmp = new File(snapshotPath);
            if (tmp.exists()) {
                FileUtils.forceDelete(new File(snapshotPath));
            }
            return true;
        } catch (final Throwable t) {
            logger.error("onSnapshotLoad  failed", t);
            return false;
        }
    }

    private void writeSnapshot(final String snapshotPath) {
        Lock lock = rocksDBEngine.getReadWriteLock().writeLock();
        lock.lock();
        try (final Checkpoint checkpoint = Checkpoint.create(rocksDBEngine.getRdb())) {
            final String tempPath = snapshotPath + "_temp";
            final File tempFile = new File(tempPath);
            FileUtils.deleteDirectory(tempFile);
            checkpoint.createCheckpoint(tempPath);
            final File snapshotFile = new File(snapshotPath);
            FileUtils.deleteDirectory(snapshotFile);
            if (!Utils.atomicMoveFile(tempFile, snapshotFile, true)) {
                throw new RuntimeException("Fail to rename [" + tempPath + "] to [" + snapshotPath + "].");
            }
        } catch (final Exception e) {
            throw new RuntimeException("writeSnapshot failed " + snapshotPath, e);
        } finally {
            lock.unlock();
        }
    }

    private void compressSnapshot(final SnapshotWriter writer, final LocalFileMetaOutter.LocalFileMeta.Builder metaBuilder, final Closure done) {
        final String writerPath = writer.getPath();
        final String outputFile = Paths.get(writerPath, SNAPSHOT_ARCHIVE).toString();
        try {
            final Checksum checksum = new CRC64();
            ZipStrategyManager.getDefault().compress(writerPath, SNAPSHOT_DIR, outputFile, checksum);
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
            if (writer.addFile(SNAPSHOT_ARCHIVE, metaBuilder.build())) {
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO, "Fail to add snapshot file: %s", writerPath));
            }
        } catch (final Throwable t) {
            logger.error("compressSnapshot failed", t);
            done.run(new Status(RaftError.EIO, "Fail to compress snapshot at %s, error is %s", writerPath, t.getMessage()));
        }
    }

    private void decompressSnapshot(final String readerPath, final LocalFileMetaOutter.LocalFileMeta meta) throws Throwable {
        final String sourceFile = Paths.get(readerPath, SNAPSHOT_ARCHIVE).toString();
        final Checksum checksum = new CRC64();
        ZipStrategyManager.getDefault().deCompress(sourceFile, readerPath, checksum);
        if (meta.hasChecksum()) {
            Requires.requireTrue(meta.getChecksum().equals(Long.toHexString(checksum.getValue())), "Snapshot checksum failed");
        }
    }

    private void readSnapshot(final String snapshotPath) {
        Lock lock = rocksDBEngine.getReadWriteLock().readLock();
        lock.lock();
        try {
            final File snapshotFile = new File(snapshotPath);
            if (!snapshotFile.exists()) {
                logger.error("Snapshot file [{}] not exists.", snapshotPath);
                return;
            }
            rocksDBEngine.closeRocksDB();
            final File dbFile = new File(rocksDBEngine.getRdbPath());
            FileUtils.deleteDirectory(dbFile);
            if (!Utils.atomicMoveFile(snapshotFile, dbFile, true)) {
                throw new RuntimeException("Fail to rename [" + snapshotPath + "] to [" + rocksDBEngine.getRdbPath() + "].");
            }
            // reopen the db
            rocksDBEngine.openRocksDB();
        } catch (final Exception e) {
            throw new RuntimeException("Fail to read snapshot from path: " + snapshotPath, e);
        } finally {
            lock.unlock();
        }
    }

    private LocalFileMetaOutter.LocalFileMeta.Builder writeMetadata(final LocalFileMetaOutter.LocalFileMeta metadata) {
        if (metadata == null) {
            return LocalFileMetaOutter.LocalFileMeta.newBuilder();
        }
        return LocalFileMetaOutter.LocalFileMeta.newBuilder()
                .setUserMeta(ByteString.copyFrom(Serializers.getDefault().writeObject(metadata)));
    }

}
