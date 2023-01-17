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

package org.apache.rocketmq.mqtt.meta.raft;

import org.apache.rocketmq.mqtt.common.model.consistency.Response;
import org.apache.rocketmq.mqtt.meta.raft.processor.WillMsgStateProcessor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.RocksDBException;

import static org.apache.rocketmq.mqtt.meta.raft.rpc.Constants.NOT_FOUND;

@RunWith(MockitoJUnitRunner.class)
public class WillMsgStateProcessorTest {

    @Test
    public void putTest() throws RocksDBException {
        WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();

        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());
        willMsgStateProcessor.closeRocksDB();
    }

    @Test
    public void getTest() throws Exception {
        WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();
        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response getResponse = willMsgStateProcessor.get(key.getBytes());
        Assert.assertEquals(value, new String(getResponse.getData().toByteArray()));
        willMsgStateProcessor.closeRocksDB();
    }

    @Test
    public void deleteTest() throws Exception {
        WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();
        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response deleteResponse = willMsgStateProcessor.delete(key.getBytes());
        Assert.assertTrue(deleteResponse.getSuccess());

        Response getResponse = willMsgStateProcessor.get(key.getBytes());
        Assert.assertEquals(NOT_FOUND, new String(getResponse.getData().toByteArray()));
        willMsgStateProcessor.closeRocksDB();
    }

    @Test
    public void compareAndPut() throws Exception {
        WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();
        String key = "k1";
        String value = "v1";
        String valueUpdate = "v2";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response responseCompareAndPut = willMsgStateProcessor.compareAndPut(key.getBytes(), value.getBytes(), valueUpdate.getBytes());
        Assert.assertTrue(responseCompareAndPut.getSuccess());


        Response responseCompareAndPut1 = willMsgStateProcessor.compareAndPut(key.getBytes(), "v5".getBytes(), valueUpdate.getBytes());
        Assert.assertFalse(responseCompareAndPut1.getSuccess());
        willMsgStateProcessor.closeRocksDB();
    }

    @Test
    public void scan() throws Exception {
        WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();
        byte CTRL_0 = '\u0000';
        byte CTRL_1 = '\u0001';
        byte CTRL_2 = '\u0002';
        String key = "k1" + CTRL_1 + "k2";
        String value = "v1";
        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        String key1 = "k1" + CTRL_1 + "k22";
        String value1 = "v11";
        Response response1 = willMsgStateProcessor.put(key1.getBytes(), value1.getBytes());
        Assert.assertTrue(response1.getSuccess());

        Response scanResponse =  willMsgStateProcessor.scan(("k1" + CTRL_0).getBytes(), ("k1" + CTRL_2).getBytes());
        Assert.assertEquals(value, scanResponse.getDataMapMap().get(key));
        Assert.assertEquals(value1, scanResponse.getDataMapMap().get(key1));
        willMsgStateProcessor.closeRocksDB();
    }

}
