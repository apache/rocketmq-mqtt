package org.apache.rocketmq.mqtt.meta.util;

import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */

@RunWith(MockitoJUnitRunner.class)
public class IpUtilTest {
    @Mock
    private ServiceConf serviceConf;

    @Test
    public void convertAllNodeAddressTest() {
        when(serviceConf.getAllNodeAddress()).thenReturn("127.0.0.1");
        when(serviceConf.getMetaPort()).thenReturn(25000);
        String allNodes = IpUtil.convertAllNodeAddress(serviceConf.getAllNodeAddress(), serviceConf.getMetaPort());
        Assert.assertEquals("127.0.0.1:25000", allNodes);

        when(serviceConf.getAllNodeAddress()).thenReturn("127.0.0.1,127.0.0.2");
        when(serviceConf.getMetaPort()).thenReturn(25000);
        String allNodes1 = IpUtil.convertAllNodeAddress(serviceConf.getAllNodeAddress(), serviceConf.getMetaPort());
        Assert.assertEquals("127.0.0.1:25000,127.0.0.2:25000", allNodes1);
    }

    @Test
    public void getLocalAddressCompatible() {
        String ip = IpUtil.getLocalAddressCompatible();
        Assert.assertNotNull(ip);
    }

}
