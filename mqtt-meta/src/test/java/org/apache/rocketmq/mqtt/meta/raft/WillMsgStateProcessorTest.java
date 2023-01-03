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

    private WillMsgStateProcessor willMsgStateProcessor = new WillMsgStateProcessor();


    @Test
    public void putTest() throws RocksDBException {
        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());
    }

    @Test
    public void getTest() throws Exception {
        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response getResponse = willMsgStateProcessor.get(key.getBytes());
        Assert.assertEquals(value, new String(getResponse.getData().toByteArray()));
    }

    @Test
    public void deleteTest() throws Exception {
        String key = "k1";
        String value = "v1";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response deleteResponse = willMsgStateProcessor.delete(key.getBytes());
        Assert.assertTrue(deleteResponse.getSuccess());

        Response getResponse = willMsgStateProcessor.get(key.getBytes());
        Assert.assertEquals(NOT_FOUND, new String(getResponse.getData().toByteArray()));
    }

    @Test
    public void compareAndPut() throws Exception {
        String key = "k1";
        String value = "v1";
        String valueUpdate = "v2";

        Response response = willMsgStateProcessor.put(key.getBytes(), value.getBytes());
        Assert.assertTrue(response.getSuccess());

        Response responseCompareAndPut = willMsgStateProcessor.compareAndPut(key.getBytes(), value.getBytes(), valueUpdate.getBytes());
        Assert.assertTrue(responseCompareAndPut.getSuccess());


        Response responseCompareAndPut1 = willMsgStateProcessor.compareAndPut(key.getBytes(), "v5".getBytes(), valueUpdate.getBytes());
        Assert.assertFalse(responseCompareAndPut1.getSuccess());

    }

    @Test
    public void scan() throws Exception {

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

    }

}
