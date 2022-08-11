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

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
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
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
public class PublishProcessor implements UpstreamProcessor {
    private static Logger logger = LoggerFactory.getLogger(PublishProcessor.class);
    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private WildcardManager wildcardManager;

    @Resource
    private FirstTopicManager firstTopicManager;

    @Resource
    RetainedPersistManager retainedPersistManager;

    @Override
    public CompletableFuture<HookResult> process(MqttMessageUpContext context, MqttMessage mqttMessage) throws RemotingException, com.alipay.sofa.jraft.error.RemotingException, ExecutionException, InterruptedException {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        boolean isEmpty = false;
        //deal empty payload
        if (ByteBufUtil.getBytes(mqttPublishMessage.content()).length == 0) {
            mqttPublishMessage = MessageUtil.dealEmptyMessage(mqttPublishMessage);
            isEmpty = true;
        }
        String msgId = MessageClientIDSetter.createUniqID();
        MqttPublishVariableHeader variableHeader = mqttPublishMessage.variableHeader();
        String originTopic = variableHeader.topicName();
        String pubTopic = TopicUtils.normalizeTopic(originTopic);
        MqttTopic mqttTopic = TopicUtils.decode(pubTopic);
        firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic());      // Check the firstTopic is existed
        Set<String> queueNames = wildcardManager.matchQueueSetByMsgTopic(pubTopic, context.getNamespace()); //According to topic to find queue
        long bornTime = System.currentTimeMillis();

        if (mqttPublishMessage.fixedHeader().isRetain()) {
            MqttPublishMessage retainedMqttPublishMessage = mqttPublishMessage.copy();
            //Change the retained flag of message that will send MQ is 0
            mqttPublishMessage = MessageUtil.removeRetainedFlag(mqttPublishMessage);
            //Keep the retained flag of message that will store meta
            Message metaMessage = MessageUtil.toMessage(retainedMqttPublishMessage);
            metaMessage.setMsgId(msgId);
            metaMessage.setBornTimestamp(bornTime);
            metaMessage.setEmpty(isEmpty);
            CompletableFuture<Boolean> storeRetainedFuture = retainedPersistManager.storeRetainedMessage(TopicUtils.normalizeTopic(metaMessage.getOriginTopic()), metaMessage);
            storeRetainedFuture.whenComplete((res, throwable) -> {
                if (throwable != null) {
                    logger.error("Store topic:{} retained message error.{}", metaMessage.getOriginTopic(), throwable);
                }
            });
        }

        Message message = MessageUtil.toMessage(mqttPublishMessage);
        message.setMsgId(msgId);
        message.setBornTimestamp(bornTime);
        message.setEmpty(isEmpty);

        CompletableFuture<StoreResult> storeResultFuture = lmqQueueStore.putMessage(queueNames, message);
        return storeResultFuture.thenCompose(storeResult -> HookResult.newHookResult(HookResult.SUCCESS, null,
            JSON.toJSONBytes(storeResult)));
    }

}
