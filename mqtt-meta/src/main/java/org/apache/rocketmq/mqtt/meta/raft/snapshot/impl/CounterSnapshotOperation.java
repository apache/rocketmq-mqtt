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

package org.apache.rocketmq.mqtt.meta.raft.snapshot.impl;

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import org.apache.rocketmq.mqtt.meta.raft.snapshot.AbstractSnapshotOperation;
import org.apache.rocketmq.mqtt.meta.util.DiskUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Snapshot processing of persistent service data for accelerated Raft protocol recovery and data synchronization.
 */
public class CounterSnapshotOperation extends AbstractSnapshotOperation {
    
    private final String snapshotDir = "alpha_persistent";
    
    private final String snapshotArchive = "alpha_persistent.zip";

    private String fileName = "snapshot";

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    
    public CounterSnapshotOperation(ReentrantReadWriteLock lock) {
        super(lock);
    }
    
    @Override
    protected boolean writeSnapshot(SnapshotWriter writer, String value) throws Exception {
        readLock.lock();
        try {
            File file = Paths.get(snapshotDir, fileName).toFile();
            try {
                DiskUtils.touch(file);
                DiskUtils.writeFile(file, value.getBytes(StandardCharsets.UTF_8), false);
            } catch (IOException e) {
                throw new Exception("fail to write snapshot");
            }
        } finally {
            readLock.unlock();
        }

        return true;
    }
    
    @Override
    protected String readSnapshot(SnapshotReader reader) throws Exception {
        final String readerPath = reader.getPath();
        final String sourceFile = Paths.get(readerPath, snapshotArchive).toString();
        String s = DiskUtils.readFile(snapshotDir, sourceFile);
        final String loadPath = Paths.get(snapshotDir, fileName).toString();
        DiskUtils.deleteDirectory(loadPath);
        return s;
    }
}
