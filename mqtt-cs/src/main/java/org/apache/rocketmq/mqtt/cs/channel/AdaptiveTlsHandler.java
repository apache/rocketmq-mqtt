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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandler;
import java.util.NoSuchElementException;
import org.apache.rocketmq.mqtt.cs.protocol.ssl.SslFactory;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class AdaptiveTlsHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveTlsHandler.class);

    private final TlsMode tlsMode;

    private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

    private final SslFactory sslFactory;

    public AdaptiveTlsHandler(TlsMode tlsMode, SslFactory sslFactory) {
        this.tlsMode = tlsMode;
        this.sslFactory = sslFactory;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {

        // Peek the current read index byte to determine if the content is starting with TLS handshake
        byte b = msg.getByte(msg.readerIndex());

        if (b == HANDSHAKE_MAGIC_CODE) {
            switch (tlsMode) {
                case DISABLED:
                    ctx.close();
                    LOGGER.warn("Clients intend to establish an SSL connection while this server is running in SSL disabled mode");
                    throw new UnsupportedOperationException("The NettyRemotingServer in SSL disabled mode doesn't support ssl client");
                case PERMISSIVE:
                case ENFORCING:
                    if (null != sslFactory.getSslContext()) {
                        ctx.pipeline()
                            .addAfter(AdaptiveTlsHandler.class.getSimpleName(), SslHandler.class.getSimpleName(),
                                sslFactory.getSslContext().newHandler(ctx.alloc()));
                        LOGGER.info("Handlers prepended to channel pipeline to establish SSL connection");
                    } else {
                        ctx.close();
                        LOGGER.error("Trying to establish an SSL connection but sslContext is null");
                    }
                    break;

                default:
                    LOGGER.warn("Unknown TLS mode");
                    break;
            }
        } else if (tlsMode == TlsMode.ENFORCING) {
            ctx.close();
            LOGGER.warn("Clients intend to establish an insecure connection while this server is running in SSL enforcing mode");
        }

        try {
            // Remove this handler
            ctx.pipeline().remove(this);
        } catch (NoSuchElementException e) {
            LOGGER.error("Error while removing TlsModeHandler", e);
        }

        // Hand over this message to the next handler.
        ctx.fireChannelRead(msg.retain());
    }


}
