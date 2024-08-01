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

package org.apache.rocketmq.mqtt.cs.protocol.coap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.ds.notify.NotifyManager;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

@Component
public class CoapRPCHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    @Resource
    private MetaPersistManager metaPersistManager;

    @Resource
    private NotifyManager notifyManager;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
            // get sender socket address
            InetSocketAddress address = packet.sender();
            // get machines
            Set<String> connectorNodes = metaPersistManager.getConnectNodeSet();
            if (connectorNodes == null || connectorNodes.isEmpty()) {
                throw new RemotingException("No Connect Nodes");
            }
            // calculate machine index to forward
            int hash = address.toString().hashCode();
            int nodeNum = Math.abs(hash % connectorNodes.size());
            List<String> nodeList = new ArrayList<>(connectorNodes);
            Collections.sort(nodeList);
            String forwardNode = nodeList.get(nodeNum);
            // forward the packet if not for localhost
            if (InetAddress.getLocalHost().getHostAddress().equals(forwardNode)) {
                ctx.fireChannelRead(packet);
            } else {
                try {
                    notifyManager.doCoapForward(forwardNode, packet);
                } finally {
                    ReferenceCountUtil.release(packet);
                }
            }
    }
}