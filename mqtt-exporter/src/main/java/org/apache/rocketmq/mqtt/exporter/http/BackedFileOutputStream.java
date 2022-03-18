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

package org.apache.rocketmq.mqtt.exporter.http;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import com.google.common.io.ByteSource;

/**
 * An {@link OutputStream} that starts buffering to a byte array, but switches to file buffering
 * once the data reaches a configurable size.
 *
 * <p>Temporary files created by this stream may live in the local filesystem until either:
 *
 * <ul>
 * <li>{@link #reset} is called (removing the data in this stream and deleting the file), or...
 * <li>this stream (or, more precisely, its {@link #asByteSource} view) is finalized during
 * garbage collection, <strong>AND</strong> this stream was not constructed with {@linkplain
 * #BackedFileOutputStream(int) the 1-arg constructor}
 * </ul>
 *
 * <p>This class is thread-safe.
 */
public class BackedFileOutputStream extends OutputStream {
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    private final File parentDirectory;
    private final int fileThreshold;
    private final boolean resetOnFinalize;
    private final ByteSource source;

    private OutputStream out;
    private MemoryOutput memory;
    private File file;

    /**
     * Creates a new instance that uses the given file threshold, and does
     * not reset the data when the {@link ByteSource} returned by
     * {@link #asByteSource} is finalized.
     *
     * @param fileThreshold the number of bytes before the stream should
     *                      switch to buffering to a file
     */
    public BackedFileOutputStream(int fileThreshold) {
        this(fileThreshold, false, null);
    }

    /**
     * Creates a new instance that uses the given file threshold, and
     * optionally resets the data when the {@link ByteSource} returned
     * by {@link #asByteSource} is finalized.
     *
     * @param fileThreshold   the number of bytes before the stream should
     *                        switch to buffering to a file
     * @param resetOnFinalize if true, the {@link #reset} method will
     *                        be called when the {@link ByteSource} returned by {@link
     *                        #asByteSource} is finalized
     */
    public BackedFileOutputStream(int fileThreshold, boolean resetOnFinalize, File parentDirectory) {
        this.parentDirectory = parentDirectory;
        this.fileThreshold = fileThreshold;
        this.resetOnFinalize = resetOnFinalize;
        this.memory = new MemoryOutput();
        this.out = memory;

        if (resetOnFinalize) {
            this.source = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return openInputStream();
                }

                @Override
                protected void finalize() {
                    try {
                        reset();
                    } catch (Throwable t) {
                        t.printStackTrace(System.err);
                    }
                }
            };
        } else {
            this.source = new ByteSource() {
                @Override
                public InputStream openStream() throws IOException {
                    return openInputStream();
                }
            };
        }
    }

    /**
     * Returns the file holding the data (possibly null).
     */
    public synchronized File getFile() {
        return file;
    }

    /**
     * Returns a readable {@link ByteSource} view of the data that has been
     * written to this stream.
     *
     * @since 15.0
     */
    public ByteSource asByteSource() {
        return source;
    }

    private synchronized InputStream openInputStream() throws IOException {
        if (file != null) {
            return new FileInputStream(file);
        } else {
            return new ByteArrayInputStream(memory.getBuffer(), 0, memory.getCount());
        }
    }

    /**
     * Calls {@link #close} if not already closed, and then resets this
     * object back to its initial state, for reuse. If data was buffered
     * to a file, it will be deleted.
     *
     * @throws IOException if an I/O error occurred while deleting the file buffer
     */
    public synchronized void reset() throws IOException {
        try {
            close();
        } finally {
            if (memory == null) {
                memory = new MemoryOutput();
            } else {
                memory.reset();
            }
            out = memory;
            if (file != null) {
                File deleteMe = file;
                file = null;
                if (!deleteMe.delete()) {
                    throw new IOException("Could not delete: " + deleteMe);
                }
            }
        }
    }

    @Override
    public synchronized void write(int b) throws IOException {
        update(1);
        out.write(b);
    }

    @Override
    public synchronized void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
        throws IOException {
        update(len);
        out.write(b, off, len);
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }

    @Override
    public synchronized void flush() throws IOException {
        out.flush();
    }

    /**
     * Checks if writing {@code len} bytes would go over threshold, and
     * switches to file buffering if so.
     */
    private void update(int len) throws IOException {
        if (memory != null && (memory.getCount() + len > fileThreshold)) {
            File temp = File.createTempFile("FileBackedOutputStream", null, parentDirectory);
            if (resetOnFinalize) {
                // Finalizers are not guaranteed to be called on system shutdown;
                // this is insurance.
                temp.deleteOnExit();
            }
            try {
                FileOutputStream transfer = new FileOutputStream(temp);
                transfer.write(memory.getBuffer(), 0, memory.getCount());
                transfer.flush();
                // We've successfully transferred the data; switch to writing to file
                out = transfer;
            } catch (IOException e) {
                temp.delete();
                throw e;
            }

            file = temp;
            memory = null;
        }
    }

    public long size() {
        if (file != null) {
            return file.length();
        } else {
            return memory.getCount();
        }
    }

    public synchronized void writeTo(OutputStream out) throws IOException {
        if (file != null) {
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(file);
                if (transferTo(fileInputStream, out) != file.length()) {
                    throw new IOException("Bug in BackedFileOutputStream");
                }
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
        } else {
            out.write(memory.getBuffer(), 0, memory.getCount());
        }
    }

    private long transferTo(FileInputStream fileInputStream, OutputStream out) throws IOException {
        Objects.requireNonNull(out, "out");
        long transferred = 0;
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int read;
        while ((read = fileInputStream.read(buffer, 0, DEFAULT_BUFFER_SIZE)) >= 0) {
            out.write(buffer, 0, read);
            transferred += read;
        }
        return transferred;
    }

    /**
     * ByteArrayOutputStream that exposes its internals.
     */
    private static class MemoryOutput extends ByteArrayOutputStream {
        byte[] getBuffer() {
            return buf;
        }

        int getCount() {
            return count;
        }
    }
}
