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

package org.apache.rocketmq.mqtt.ds.test.notify;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.ds.notify.NotifyManager;
import org.apache.rocketmq.mqtt.ds.notify.NotifyRetryManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class TestNotifyRetryManager {

    @Mock
    private NotifyManager notifyManager;

    private NotifyRetryManager notifyRetryManager;

    @Test
    public void test() throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        notifyRetryManager = new NotifyRetryManager();
        FieldUtils.writeDeclaredField(notifyRetryManager, "notifyManager", notifyManager, true);

        MessageExt messageExt = new MessageExt();
        messageExt.putUserProperty(Constants.PROPERTY_MQTT_MSG_EVENT_RETRY_NODE, "test");
        messageExt.putUserProperty(Constants.PROPERTY_MQTT_MSG_EVENT_RETRY_TIME, "1");
        MessageEvent messageEvent = new MessageEvent();
        messageExt.setBody(JSON.toJSONString(Collections.singleton(messageEvent)).getBytes(StandardCharsets.UTF_8));

        MethodUtils.invokeMethod(notifyRetryManager, true, "doRetryNotify", messageExt);
    }

}
