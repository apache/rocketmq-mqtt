/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.meta.raft.snapshot.impl;

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.AbstractSnapshotOperation;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.LocalFileMeta;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.Writer;
import org.apache.rocketmq.mqtt.meta.util.DiskUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.Checksum;

/**
 * Snapshot processing of persistent service data for accelerated Raft protocol recovery and data synchronization.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author xiweng.yy
 */
public class AlphaSnapshotOperation extends AbstractSnapshotOperation {
    
    private static final String NAMING_SNAPSHOT_SAVE = AlphaSnapshotOperation.class.getSimpleName() + ".SAVE";
    
    private static final String NAMING_SNAPSHOT_LOAD = AlphaSnapshotOperation.class.getSimpleName() + ".LOAD";
    
    private final String snapshotDir = "alpha_persistent";
    
    private final String snapshotArchive = "alpha_persistent.zip";

    private String fileName = "snapshot";

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private Map<String, String> value;

    
    public AlphaSnapshotOperation(ReentrantReadWriteLock lock, Map<String, String> value) {
        super(lock);
        this.value = value;
    }
    
    @Override
    protected boolean writeSnapshot(SnapshotWriter writer) throws Exception {
        readLock.lock();
        try {
            File file = Paths.get(snapshotDir, fileName).toFile();
            try {
                DiskUtils.touch(file);
                DiskUtils.writeFile(file, value.toString().getBytes(StandardCharsets.UTF_8), false);
            } catch (IOException e) {
                throw new Exception("fail to write snapshot");
            }
        } finally {
            readLock.unlock();
        }

        final LocalFileMeta meta = new LocalFileMeta();
        final Writer wCtx = new Writer(writer.getPath());
        return wCtx.addFile(snapshotArchive, meta);
    }
    
    @Override
    protected boolean readSnapshot(SnapshotReader reader) throws Exception {
        final String readerPath = reader.getPath();
        final String sourceFile = Paths.get(readerPath, snapshotArchive).toString();
        final Checksum checksum = new CRC64();
        DiskUtils.decompress(sourceFile, readerPath, checksum);
        String s = DiskUtils.readFile(snapshotDir, fileName);
        Map<String, String> map = new HashMap<>();
        map.put("alpha", s);
        value = map;
        final String loadPath = Paths.get(snapshotDir, fileName).toString();
        DiskUtils.deleteDirectory(loadPath);
        return true;
    }
    
    @Override
    protected String getSnapshotSaveTag() {
        return NAMING_SNAPSHOT_SAVE;
    }
    
    @Override
    protected String getSnapshotLoadTag() {
        return NAMING_SNAPSHOT_LOAD;
    }
}
