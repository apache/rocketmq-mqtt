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

package org.apache.rocketmq.mqtt.cs.protocol.rpc;


import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.mqtt.common.model.MessageEvent;
import org.apache.rocketmq.mqtt.common.model.RpcCode;
import org.apache.rocketmq.mqtt.common.model.RpcHeader;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import org.apache.rocketmq.mqtt.cs.session.notify.MessageNotifyAction;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.List;


@Component
public class RpcPacketDispatcher implements NettyRequestProcessor {
    private static Logger logger = LoggerFactory.getLogger(RpcPacketDispatcher.class);

    @Resource
    private MessageNotifyAction messageNotifyAction;

    @Resource
    private ChannelManager channelManager;

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(RpcCode.SUCCESS, null);
        response.setOpaque(request.getOpaque());
        int code = request.getCode();
        try {
            if (RpcCode.CMD_NOTIFY_MQTT_MESSAGE == code) {
                doNotify(request);
            } else if (RpcCode.CMD_CLOSE_CHANNEL == code) {
                closeChannel(request);
            }
        } catch (Throwable t) {
            logger.error("", t);
            response.setCode(RpcCode.FAIL);
        }
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private void doNotify(RemotingCommand request) {
        String payload = new String(request.getBody(), StandardCharsets.UTF_8);
        List<MessageEvent> events = JSONObject.parseArray(payload, MessageEvent.class);
        messageNotifyAction.notify(events);
    }

    private void closeChannel(RemotingCommand request) {
        String channelId = request.getExtFields() != null ?
                request.getExtFields().get(RpcHeader.MQTT_CHANNEL_ID) : null;
        channelManager.closeConnect(channelId, request.getRemark());
    }

}
