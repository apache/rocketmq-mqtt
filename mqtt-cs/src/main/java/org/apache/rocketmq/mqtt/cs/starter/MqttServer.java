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

package org.apache.rocketmq.mqtt.cs.starter;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.rocketmq.mqtt.cs.channel.ConnectHandler;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketDispatcher;
import org.apache.rocketmq.mqtt.cs.protocol.ssl.SslFactory;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketServerHandler;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;

@Service
public class MqttServer {
    private static Logger logger = LoggerFactory.getLogger(MqttServer.class);

    private ServerBootstrap serverBootstrap = new ServerBootstrap();
    private ServerBootstrap wsServerBootstrap = new ServerBootstrap();
    private ServerBootstrap tlsServerBootstrap = new ServerBootstrap();

    @Resource
    private ConnectHandler connectHandler;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private MqttPacketDispatcher mqttPacketDispatcher;

    @Resource
    private WebSocketServerHandler webSocketServerHandler;

    @Resource
    private SslFactory sslFactory;

    @PostConstruct
    public void init() throws Exception {
        start();
        startWs();
        startTls();
    }

    private void start() {
        int port = connectConf.getMqttPort();
        serverBootstrap
            .group(new NioEventLoopGroup(connectConf.getNettySelectThreadNum()), new NioEventLoopGroup(connectConf.getNettyWorkerThreadNum()))
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(port))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("connectHandler", connectHandler);
                    pipeline.addLast("decoder", new MqttDecoder(connectConf.getMaxPacketSizeInByte()));
                    pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("dispatcher", mqttPacketDispatcher);
                }
            });
        serverBootstrap.bind();
        logger.warn("start mqtt server , port:{}", port);
    }

    private void startTls() {
        if (!connectConf.isEnableTlsSever()) {
            return;
        }

        int tlsPort = connectConf.getMqttTlsPort();
        tlsServerBootstrap
            .group(new NioEventLoopGroup(connectConf.getNettySelectThreadNum()), new NioEventLoopGroup(connectConf.getNettyWorkerThreadNum()))
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(tlsPort))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("sslHandler", new SslHandler(sslFactory.buildSslEngine(ch)));
                    pipeline.addLast("connectHandler", connectHandler);
                    pipeline.addLast("decoder", new MqttDecoder(connectConf.getMaxPacketSizeInByte()));
                    pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("dispatcher", mqttPacketDispatcher);
                }
            });
        tlsServerBootstrap.bind();
        logger.warn("start mqtt tls server , port:{}", tlsPort);
    }

    private void startWs() {
        int port = connectConf.getMqttWsPort();
        wsServerBootstrap
            .group(new NioEventLoopGroup(connectConf.getNettySelectThreadNum()), new NioEventLoopGroup(connectConf.getNettyWorkerThreadNum()))
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(port))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("connectHandler", connectHandler);
                    pipeline.addLast("http-codec", new HttpServerCodec(1024, 32 * 1024, connectConf.getMaxPacketSizeInByte() * 2, true));
                    pipeline.addLast("aggregator", new HttpObjectAggregator(connectConf.getMaxPacketSizeInByte() * 2));
                    pipeline.addLast("http-chunked", new ChunkedWriteHandler());
                    pipeline.addLast("websocket-handler", webSocketServerHandler);
                    pipeline.addLast("websocket-encoder", new WebSocketEncoder());
                    pipeline.addLast("decoder", new MqttDecoder(connectConf.getMaxPacketSizeInByte()));
                    pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    pipeline.addLast("dispatcher", mqttPacketDispatcher);
                }
            });
        wsServerBootstrap.bind();
        logger.warn("start mqtt ws server , port:{}", port);
    }

}
