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

package org.apache.rocketmq.mqtt.ds.event.processor;

import com.alibaba.fastjson.JSON;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.hook.ClientEventHook;
import org.apache.rocketmq.mqtt.common.hook.ClientEventHookManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.exporter.collector.MqttMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.rocketmq.mqtt.common.model.Constants.CLIENT_EVENT_FIRST_TOPIC;
import static org.apache.rocketmq.mqtt.common.model.Constants.CLIENT_EVENT_ORIGIN_TOPIC;
import static org.apache.rocketmq.mqtt.common.model.Constants.PROPERTY_NAMESPACE;

@Component
public class ClientEventProcessor extends ClientEventHook {
  private static Logger logger = LoggerFactory.getLogger(ClientEventProcessor.class);

  @Resource
  private ClientEventHookManager clientEventHookManager;

  @Resource
  private LmqQueueStore lmqQueueStore;

  @Resource
  private FirstTopicManager firstTopicManager;

  @Resource
  private WildcardManager wildcardManager;

  @PostConstruct
  @Override
  public void register() {
    clientEventHookManager.addHook(this);
  }

  @Override
  public CompletableFuture<HookResult> process(List<MqttPublishMessage> eventPublishMessages) {
    CompletableFuture<StoreResult> r = put(eventPublishMessages);
    return r.thenCompose(storeResult -> HookResult.newHookResult(HookResult.SUCCESS, null,
        JSON.toJSONBytes(storeResult)));
  }

  private CompletableFuture<StoreResult> put(List<MqttPublishMessage> eventPublishMessages) {
    firstTopicManager.checkFirstTopicIfCreated(CLIENT_EVENT_FIRST_TOPIC);
    String pubTopic = TopicUtils.normalizeTopic(CLIENT_EVENT_ORIGIN_TOPIC);
    Set<String> queueNames = wildcardManager.matchQueueSetByMsgTopic(pubTopic, PROPERTY_NAMESPACE);

    List<Message> messageList = new ArrayList<>(eventPublishMessages.size());
    for (MqttPublishMessage eventPubMsg : eventPublishMessages) {
      String msgId = MessageClientIDSetter.createUniqID();
      long bornTime = System.currentTimeMillis();

      Message message = MessageUtil.toMessage(eventPubMsg);
      message.setMsgId(msgId);
      message.setBornTimestamp(bornTime);
      message.setEmpty(false);
      messageList.add(message);
      collectWriteBytes(message.getFirstTopic(), message.getPayload().length);
    }

    return lmqQueueStore.putMessage(queueNames, messageList);
  }

  private void collectWriteBytes(String topic, int length) {
    try {
      MqttMetricsCollector.collectEventReadWriteBytes(length, topic, "put");
    } catch (Throwable e) {
      logger.error("Collect prometheus error", e);
    }
  }

}