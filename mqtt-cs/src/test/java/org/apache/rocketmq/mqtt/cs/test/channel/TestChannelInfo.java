/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.cs.test.channel;

import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

public class TestChannelInfo {

    @Test
    public void test() {
        NioSocketChannel channel = new NioSocketChannel();
        String extDataStr = "{\"test\":\"extData\"}";

        // test 'update/check/get/encode' of 'ExtData'
        Assert.assertFalse(ChannelInfo.updateExtData(channel, ""));
        Assert.assertFalse(ChannelInfo.checkExtDataChange(channel));
        Assert.assertEquals(0, ChannelInfo.getExtData(channel).size());
        // update 'ExtData'
        Assert.assertTrue(ChannelInfo.updateExtData(channel, extDataStr));
        Assert.assertTrue(ChannelInfo.checkExtDataChange(channel));
        Assert.assertEquals(1, ChannelInfo.getExtData(channel).size());
        Assert.assertEquals(extDataStr, ChannelInfo.encodeExtData(channel));

        // test 'getId'
        Assert.assertNotNull(ChannelInfo.getId(channel));

        // test 'set/get CleanSessionFlag'
        ChannelInfo.setCleanSessionFlag(channel, Boolean.FALSE);
        Assert.assertFalse(ChannelInfo.getCleanSessionFlag(channel));

        // test 'set/get ClientId'
        String clientId = "testExtData";
        ChannelInfo.setClientId(channel, clientId);
        Assert.assertEquals(clientId, ChannelInfo.getClientId(channel));

        // test 'set/get ChannelLifeCycle'
        ChannelInfo.setChannelLifeCycle(channel, System.currentTimeMillis());
        Assert.assertNotEquals(Long.MAX_VALUE, ChannelInfo.getChannelLifeCycle(channel));

        // test 'set/get/remove Future'
        String futureKey = "futureKey";
        ChannelInfo.setFuture(channel, futureKey, new CompletableFuture<>());
        Assert.assertNotNull(ChannelInfo.getFuture(channel, futureKey));
        ChannelInfo.removeFuture(channel, futureKey);
        Assert.assertNull(ChannelInfo.getFuture(channel, futureKey));

        // test 'touch/getLastTouch'
        Assert.assertEquals(0, ChannelInfo.getLastTouch(channel));
        ChannelInfo.touch(channel);
        Assert.assertNotEquals(0, ChannelInfo.getLastTouch(channel));

        // test 'lastActive/getLastActive'
        ChannelInfo.lastActive(channel, System.currentTimeMillis());
        Assert.assertNotEquals(0, ChannelInfo.getLastActive(channel));

        // test 'set/get RemoteIP'
        ChannelInfo.setRemoteIP(channel, "");
        Assert.assertEquals("", ChannelInfo.getRemoteIP(channel));

        // test 'set/get KeepLive/isExpired'
        Assert.assertTrue(ChannelInfo.isExpired(channel));
        ChannelInfo.setKeepLive(channel, -1);
        Assert.assertTrue(ChannelInfo.isExpired(channel));

        // test 'set/get Owner/Namespace'
        String ownerNamespc = "channelInfo";
        ChannelInfo.setOwner(channel, ownerNamespc);
        ChannelInfo.setNamespace(channel, ownerNamespc);
        Assert.assertEquals(ownerNamespc, ChannelInfo.getOwner(channel));
        Assert.assertEquals(ownerNamespc, ChannelInfo.getNamespace(channel));

        // test 'clear'
        ChannelInfo.clear(channel);
        Assert.assertEquals(ownerNamespc, ChannelInfo.getNamespace(channel));
        Assert.assertEquals(0, ChannelInfo.getExtData(channel).size());

        if (channel.isActive()) {
            channel.close();
        }
    }
}