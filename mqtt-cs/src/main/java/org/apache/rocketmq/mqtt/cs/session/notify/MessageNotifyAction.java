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

package org.apache.rocketmq.mqtt.cs.session.notify;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.cs.session.QueueFresh;
import org.apache.rocketmq.mqtt.cs.session.Session;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;
import org.apache.rocketmq.mqtt.cs.session.loop.QueueCache;
import org.apache.rocketmq.mqtt.cs.session.loop.SessionLoop;
import org.apache.rocketmq.mqtt.cs.session.loop.CoapSessionLoop;
import org.apache.rocketmq.mqtt.cs.session.match.MatchAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Component
public class MessageNotifyAction {
    private static Logger logger = LoggerFactory.getLogger(MessageNotifyAction.class);

    @Resource
    private MatchAction matchAction;

    @Resource
    private SessionLoop sessionLoop;

    @Resource
    private CoapSessionLoop coapSessionLoop;

    @Resource
    private QueueCache queueCache;

    @Resource
    private QueueFresh queueFresh;

    public void notify(List<MessageEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        for (MessageEvent event : events) {
            Set<Pair<Session, Subscription>> result = matchAction.matchClients(
                    TopicUtils.normalizeTopic(event.getPubTopic()), event.getNamespace());
            if (result == null || result.isEmpty()) {
                continue;
            }
            for (Pair<Session, Subscription> pair : result) {
                Session session = pair.getLeft();
                Subscription subscription = pair.getRight();
                Set<Queue> set = queueFresh.freshQueue(session, subscription);
                if (set == null || set.isEmpty()) {
                    continue;
                }
                for (Queue queue : set) {
                    if (isTargetQueue(queue, event)) {
                        queueCache.refreshCache(Pair.of(queue, session));
                        sessionLoop.notifyPullMessage(session, subscription, queue);
                    }
                }
            }
        }
        for (MessageEvent event : events) {
            Set<CoapSession> coapResult = matchAction.matchCoapClients(TopicUtils.normalizeTopic(event.getPubTopic()));
            if (coapResult == null || coapResult.isEmpty()) {
                continue;
            }
            for (CoapSession coapSession : coapResult) {
                Set<Queue> set = queueFresh.freshQueue(coapSession);
                if (set == null || set.isEmpty()) {
                    continue;
                }
                for (Queue queue : set) {
                    if (isTargetQueue(queue, event)) {
                        // todo: add queueCache
                        coapSessionLoop.notifyPullMessage(coapSession, queue);
                    }
                }
            }
        }
    }

    private boolean isTargetQueue(Queue queue, MessageEvent event) {
        return Objects.equals(queue.getBrokerName(), event.getBrokerName());
    }

}
