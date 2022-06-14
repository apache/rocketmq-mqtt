package org.apache.rocketmq.mqtt.meta.util;

import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class KVTest {

    @Mock
    private MetaClient metaClient;

    @Test
    public void kvTest(){
        WillMessage willMessage = new WillMessage("offline", "i am offline".getBytes(), false, 0);
        metaClient.bPut("will"+"%%%"+"offline", "123".getBytes());

        byte[] bytes = metaClient.bGet("will"+"%%%"+"offline");
        System.out.println(bytes.toString());
    }
}
