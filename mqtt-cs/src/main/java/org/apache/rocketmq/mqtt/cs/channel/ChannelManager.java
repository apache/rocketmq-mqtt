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

package org.apache.rocketmq.mqtt.cs.channel;

import io.netty.channel.Channel;


public interface ChannelManager {

    /**
     * addChannel
     *
     * @param channel
     */
    void addChannel(Channel channel);

    /**
     * closeConnect
     *
     * @param channel
     * @param from
     * @param reason
     */
    void closeConnect(Channel channel, ChannelCloseFrom from, String reason);

    void closeConnect(Channel channel, ChannelCloseFrom from, String reason, byte reasonCode);

    void closeConnectWithProtocolError(Channel channel, String reason);

    /**
     *  closeConnect
     * @param channelId
     * @param reason
     */
    void closeConnect(String channelId, String reason);

    /**
     *  get channel by Id
     * @param channelId
     * @return
     */
    Channel getChannelById(String channelId);

    /**
     * totalConn
     *
     * @return
     */
    int totalConn();

    /**
     * Refills the publish receive quota for the given channel.
     * This method is typically called after a publish acknowledgement or other scenarios that refill the quota.
     * @param channel The channel for which the quota needs to be refilled.
     * @return true if the quota was successfully refilled, false otherwise.
     */
    boolean publishReceiveRefill(Channel channel);

    /**
     * Tries to acquire the publish receive quota for the given channel.
     * This method is typically called before a publish operation.
     * @param channel The channel for which the quota needs to be acquired.
     * @return true if the quota was successfully acquired, false otherwise.
     */
    boolean publishReceiveTryAcquire(Channel channel);

    /**
     * Refills the publish send quota for the given channel.
     * This method is typically called after a publish acknowledgement or other scenarios that refill the quota.
     * @param channel The channel for which the quota needs to be refilled.
     * @return true if the quota was successfully refilled, false otherwise.
     */
    boolean publishSendRefill(Channel channel);

    /**
     * Tries to acquire the publish send quota for the given channel.
     * This method is typically called before a publish operation.
     * @param channel The channel for which the quota needs to be acquired.
     * @return true if the quota was successfully acquired, false otherwise.
     */
    boolean publishSendTryAcquire(Channel channel);
}
