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

package org.apache.rocketmq.mqtt.meta.util;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

/**
 * IO operates on the utility class.
 */
public final class DiskUtils {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskUtils.class);
    
    private static final String NO_SPACE_CN = "no space";
    
    private static final String NO_SPACE_EN = "No space left on device";
    
    private static final String DISK_QUATA_CN = "out of disk";
    
    private static final String DISK_QUATA_EN = "Disk quota exceeded";
    
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    
    private static final CharsetDecoder DECODER = CHARSET.newDecoder();


    public static void touch(File file) throws IOException {
        FileUtils.touch(file);
    }

    public static String readFile(String path, String fileName) {
        File file = openFile(path, fileName);
        if (file.exists()) {
            return readFile(file);
        }
        return null;
    }

    public static String readFile(File file) {
        try (FileChannel fileChannel = new FileInputStream(file).getChannel()) {
            StringBuilder text = new StringBuilder();
            ByteBuffer buffer = ByteBuffer.allocate(4096);
            CharBuffer charBuffer = CharBuffer.allocate(4096);
            while (fileChannel.read(buffer) != -1) {
                buffer.flip();
                DECODER.decode(buffer, charBuffer, false);
                charBuffer.flip();
                while (charBuffer.hasRemaining()) {
                    text.append(charBuffer.get());
                }
                buffer.clear();
                charBuffer.clear();
            }
            return text.toString();
        } catch (IOException e) {
            return null;
        }
    }

    public static boolean writeFile(File file, byte[] content, boolean append) {
        try (FileChannel fileChannel = new FileOutputStream(file, append).getChannel()) {
            ByteBuffer buffer = ByteBuffer.wrap(content);
            fileChannel.write(buffer);
            return true;
        } catch (IOException ioe) {
            if (ioe.getMessage() != null) {
                String errMsg = ioe.getMessage();
                if (NO_SPACE_CN.equals(errMsg) || NO_SPACE_EN.equals(errMsg) || errMsg.contains(DISK_QUATA_CN) || errMsg
                        .contains(DISK_QUATA_EN)) {
                    LOGGER.warn("out of disk");
                    System.exit(0);
                }
            }
        }
        return false;
    }
    
    public static void deleteDirectory(String path) throws IOException {
        FileUtils.deleteDirectory(new File(path));
    }
    
    public static File openFile(String path, String fileName) {
        return openFile(path, fileName, false);
    }

    public static File openFile(String path, String fileName, boolean rewrite) {
        File directory = new File(path);
        boolean mkdirs = true;
        if (!directory.exists()) {
            mkdirs = directory.mkdirs();
        }
        if (!mkdirs) {
            LOGGER.error("[DiskUtils] can't create directory");
            return null;
        }
        File file = new File(path, fileName);
        try {
            boolean create = true;
            if (!file.exists()) {
                file.createNewFile();
            }
            if (file.exists()) {
                if (rewrite) {
                    file.delete();
                } else {
                    create = false;
                }
            }
            if (create) {
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return file;
    }
    
}
