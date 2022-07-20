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

package org.apache.rocketmq.mqtt.meta.raft.snapshot;

import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import org.apache.rocketmq.mqtt.meta.util.RaftExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

/**
 * Abstract snapshot operation.
 *
 */
public abstract class AbstractSnapshotOperation implements SnapshotOperation {

    private static Logger logger = LoggerFactory.getLogger(AbstractSnapshotOperation.class);
    
    private final ReentrantReadWriteLock.WriteLock writeLock;
    
    public AbstractSnapshotOperation(ReentrantReadWriteLock lock) {
        this.writeLock = lock.writeLock();
    }
    
    @Override
    public void onSnapshotSave(SnapshotWriter writer, BiConsumer<Boolean, Throwable> callFinally) {
        RaftExecutor.doSnapshot(() -> {
            final Lock lock = writeLock;
            lock.lock();
            try {
                callFinally.accept(writeSnapshot(writer), null);
            } catch (Throwable t) {
                logger.error("Fail to compress snapshot, path={}, file list={}.", writer.getPath(),
                        writer.listFiles(), t);
                callFinally.accept(false, t);
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        final Lock lock = writeLock;
        lock.lock();
        try {
            return readSnapshot(reader);
        } catch (final Throwable t) {
            logger.error("Fail to load snapshot, path={}, file list={}.", reader.getPath(), reader.listFiles(), t);
            return false;
        } finally {
            lock.unlock();
        }
    }

    protected abstract boolean writeSnapshot(SnapshotWriter writer) throws Exception;

    protected abstract boolean readSnapshot(SnapshotReader reader) throws Exception;
}
