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

package org.apache.rocketmq.mqtt.common.test;

import org.apache.rocketmq.mqtt.common.util.NamespaceUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNamespaceUtil {

    String resource;
    String namespace;
    String originResource;

    @Before
    public void Before() {
        resource = "namespaceTest001%originResourceTest001";
        namespace = "namespaceTest001";
        originResource = "originResourceTest001";
    }

    @Test
    public void testDecodeOriginResource() {
        Assert.assertEquals(originResource, NamespaceUtil.decodeOriginResource(resource));
    }

    @Test
    public void testEncodeToNamespaceResource() {
        Assert.assertEquals(resource, NamespaceUtil.encodeToNamespaceResource(namespace, originResource));
    }

    @Test
    public void testDecodeMqttNamespaceIdFromKey() {
        Assert.assertEquals(namespace, NamespaceUtil.decodeMqttNamespaceIdFromKey(resource));
    }

    @Test
    public void testDecodeMqttNamespaceIdFromClientId() {
        Assert.assertEquals(namespace, NamespaceUtil.decodeMqttNamespaceIdFromClientId(resource));
    }

    @Test
    public void testDecodeStoreNamespaceIdFromTopic() {
        Assert.assertEquals(namespace, NamespaceUtil.decodeStoreNamespaceIdFromTopic(resource));
    }

    @Test
    public void testDecodeNamespaceId() {
        Assert.assertEquals(namespace, NamespaceUtil.decodeNamespaceId(resource));
    }

}
