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

package org.apache.rocketmq.mqtt.common.facade;

import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.common.model.QueueOffset;
import org.apache.rocketmq.mqtt.common.model.Subscription;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface LmqOffsetStore {
    /**
     *  save lmq offset
     * @param clientId
     * @param offsetMap
     */
    void save(String clientId, Map<Subscription, Map<Queue, QueueOffset>> offsetMap);

    /**
     *  get lmq offset
     * @param clientId
     * @param subscription
     * @return
     */
    CompletableFuture<Map<Queue, QueueOffset>> getOffset(String clientId, Subscription subscription);
}
