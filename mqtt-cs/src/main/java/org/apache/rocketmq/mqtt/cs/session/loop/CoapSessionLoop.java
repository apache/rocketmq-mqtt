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
package org.apache.rocketmq.mqtt.cs.session.loop;

import org.apache.rocketmq.mqtt.common.model.Queue;
import org.apache.rocketmq.mqtt.cs.session.CoapSession;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public interface CoapSessionLoop {

    /**
     * Add one coap session.
     *
     * @param session
     * @param future
     */
    void addSession(CoapSession session, CompletableFuture<Void> future);

    /**
     * Get one coap session by ip-port.
     *
     * @param address
     * @return
     */
    CoapSession getSession(InetSocketAddress address);

    /**
     * Remove one coap session.
     *
     * @param address
     * @return
     */
    CoapSession removeSession(InetSocketAddress address);

    /**
     * notify to pull message from queue
     *
     * @param session
     * @param queue
     */
    void notifyPullMessage(CoapSession session, Queue queue);
}
