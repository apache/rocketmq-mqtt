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

package org.apache.rocketmq.mqtt.ds.notify;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.ds.mq.MqFactory;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class NotifyRetryManager {
    private static Logger logger = LoggerFactory.getLogger(NotifyRetryManager.class);
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @Resource
    private NotifyManager notifyManager;

    @Resource
    private ServiceConf serviceConf;

    @PostConstruct
    public void init() throws MQClientException {
        defaultMQPushConsumer = MqFactory.buildDefaultMQPushConsumer(MixAll.CID_RMQ_SYS_PREFIX + "notify_retry",
                serviceConf.getProperties(), new RetryNotify());
        defaultMQPushConsumer.subscribe(serviceConf.getEventNotifyRetryTopic(), "*");
        defaultMQPushConsumer.start();
    }

    class RetryNotify implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            try {
                for (MessageExt messageExt : msgs) {
                    doRetryNotify(messageExt);
                }
            } catch (Exception e) {
                logger.error("", e);
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    private void doRetryNotify(MessageExt messageExt)
            throws  InterruptedException, RemotingException, MQClientException, MQBrokerException {
        String payload = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        Set<MessageEvent> events = new HashSet<>(JSONObject.parseArray(payload, MessageEvent.class));
        String node = messageExt.getUserProperty(Constants.PROPERTY_MQTT_MSG_EVENT_RETRY_NODE);
        String retryTime = messageExt.getUserProperty(Constants.PROPERTY_MQTT_MSG_EVENT_RETRY_TIME);
        if (StringUtils.isBlank(node)) {
            return;
        }
        if (notifyManager.doNotify(node, events)) {
            return;
        }
        notifyManager.sendEventRetryMsg(events, 2, node, retryTime != null ? Integer.parseInt(retryTime) : 1);
    }
}
