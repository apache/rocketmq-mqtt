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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;


@ChannelHandler.Sharable
@Component
public class ConnectHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(ConnectHandler.class);

    private final List<String> simpleExceptions = Collections.singletonList("Connection reset by peer");

    @Resource
    private ChannelManager channelManager;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelActive();
        channelManager.addChannel(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.CLIENT, "be closed");
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause.getMessage() == null || !simpleExceptions.contains(cause.getMessage())) {
            logger.error("exceptionCaught {}", ctx.channel(), cause);
        }
        channelManager.closeConnect(ctx.channel(), ChannelCloseFrom.SERVER, cause.getMessage());
    }

}
