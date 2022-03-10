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

package org.apache.rocketmq.mqtt.ds.upstream.processor;

import com.alibaba.fastjson.JSONObject;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.ds.upstream.UpstreamProcessor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
public class PublishProcessor implements UpstreamProcessor {

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private WildcardManager wildcardManager;

    @Resource
    private FirstTopicManager firstTopicManager;


    @Override
    public CompletableFuture<HookResult> process(MqttMessageUpContext context, MqttMessage mqttMessage) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        String msgId = MessageClientIDSetter.createUniqID();
        MqttPublishVariableHeader variableHeader = mqttPublishMessage.variableHeader();
        String originTopic = variableHeader.topicName();
        String pubTopic = TopicUtils.normalizeTopic(originTopic);
        MqttTopic mqttTopic = TopicUtils.decode(pubTopic);
        firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic());
        Set<String> queueNames = wildcardManager.matchQueueSetByMsgTopic(pubTopic, context.getNamespace());
        Message message = MessageUtil.toMessage(mqttPublishMessage);
        message.setMsgId(msgId);
        message.setBornTimestamp(System.currentTimeMillis());
        message.setFirstTopic(mqttTopic.getFirstTopic());
        CompletableFuture<StoreResult> storeResult = lmqQueueStore.putMessage(queueNames, message);
        return storeResult.thenCompose(storeResult1 -> HookResult.newHookResult(HookResult.SUCCESS, null,
                JSONObject.toJSONString(storeResult1).getBytes(StandardCharsets.UTF_8)));
    }

}
