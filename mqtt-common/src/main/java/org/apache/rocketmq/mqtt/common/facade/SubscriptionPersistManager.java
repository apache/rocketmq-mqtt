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

package org.apache.rocketmq.mqtt.common.facade;

import org.apache.rocketmq.mqtt.common.model.Subscription;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriptionPersistManager {
    /**
     * loadSubscriptions
     *
     * @param clientId
     * @return
     */
    CompletableFuture<Set<Subscription>> loadSubscriptions(String clientId);

    /**
     * loadSubscribers
     *
     * @param topic
     * @return
     */
    CompletableFuture<Set<String>> loadSubscribers(String topic);

    /**
     * saveSubscriptions
     *
     * @param clientId
     * @param subscriptions
     */
    void saveSubscriptions(String clientId, Set<Subscription> subscriptions);

    /**
     * saveSubscribers
     *
     * @param topic
     * @param clientIds
     */
    void saveSubscribers(String topic, Set<String> clientIds);

    /**
     * removeSubscriptions
     *
     * @param clientId
     * @param subscriptions
     */
    void removeSubscriptions(String clientId, Set<Subscription> subscriptions);

    /**
     * removeSubscriptions
     *
     * @param topic
     * @param clientIds
     */
    void removeSubscribers(String topic, Set<String> clientIds);
}
