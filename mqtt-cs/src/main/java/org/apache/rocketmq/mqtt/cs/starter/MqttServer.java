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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.rocketmq.mqtt.cs.channel.ChannelManager;
import javax.annotation.PreDestroy;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.mqtt.cs.channel.ConnectHandler;
import org.apache.rocketmq.mqtt.cs.channel.AdaptiveTlsHandler;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.ChannelPipelineLazyInit;
import org.apache.rocketmq.mqtt.cs.protocol.MqttVersionHandler;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt.MqttPacketDispatcher;
import org.apache.rocketmq.mqtt.cs.protocol.mqtt5.Mqtt5PacketDispatcher;
import org.apache.rocketmq.mqtt.cs.protocol.ssl.SslFactory;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketServerHandler;
import org.apache.rocketmq.mqtt.cs.protocol.ws.WebSocketEncoder;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.net.InetSocketAddress;

@Service
public class MqttServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttServer.class);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final ServerBootstrap wsServerBootstrap = new ServerBootstrap();
    private final ServerBootstrap tlsServerBootstrap = new ServerBootstrap();

    private Bootstrap coapBootstrap = new Bootstrap();

    private final Bootstrap quicBootstrap = new Bootstrap();

    @Resource
    private ConnectHandler connectHandler;

    @Resource
    private ConnectConf connectConf;

    @Resource
    private MqttPacketDispatcher mqttPacketDispatcher;

    @Resource
    private Mqtt5PacketDispatcher mqtt5PacketDispatcher;

    @Resource
    private WebSocketServerHandler webSocketServerHandler;

    @Resource
    private SslFactory sslFactory;


    @Resource
    private ChannelManager channelManager;
    private NioEventLoopGroup acceptorEventLoopGroup;

    private NioEventLoopGroup workerEventLoopGroup;

    private AdaptiveTlsHandler adaptiveTlsHandler;

    @PostConstruct
    public void init() throws Exception {
        acceptorEventLoopGroup = new NioEventLoopGroup(connectConf.getNettySelectorThreadNum());
        workerEventLoopGroup = new NioEventLoopGroup(connectConf.getNettyWorkerThreadNum());

        adaptiveTlsHandler = new AdaptiveTlsHandler(TlsMode.PERMISSIVE, sslFactory);

        start();
        startTls();

        startWs();

        startCoap();

        // QUIC over DTLS
        if (connectConf.isEnableQuic()) {
            startQuic();
        }
    }

    @PreDestroy
    public void shutdown() {
        if (null != acceptorEventLoopGroup) {
            acceptorEventLoopGroup.shutdownGracefully();
        }

        if (null != workerEventLoopGroup) {
            workerEventLoopGroup.shutdownGracefully();
        }
    }

    private void start() {
        int port = connectConf.getMqttPort();
        serverBootstrap
            .group(acceptorEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(port))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    assembleHandlerPipeline(pipeline);
                }
            });
        serverBootstrap.bind();
        LOGGER.info("MQTT server for TCP started, listening: {}", port);
    }

    private void startTls() {
        if (!connectConf.isEnableTlsSever()) {
            return;
        }

        int tlsPort = connectConf.getMqttTlsPort();
        tlsServerBootstrap
            .group(acceptorEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(tlsPort))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast("sslHandler", new SslHandler(sslFactory.buildSslEngine(ch)));
                    assembleHandlerPipeline(pipeline);
                }
            });
        tlsServerBootstrap.bind();
        LOGGER.info("MQTT server for TCP over TLS started, listening: {}", tlsPort);
    }

    private void assembleHandlerPipeline(ChannelPipeline pipeline) {
        pipeline.addLast("connectHandler", connectHandler);
        pipeline.addLast("versionHandler", new MqttVersionHandler(channelManager, new ChannelPipelineLazyInit() {
            @Override
            public void init(ChannelPipeline channelPipeline, MqttVersion mqttVersion) {
                channelPipeline.addLast("decoder", new MqttDecoder(connectConf.getMaxPacketSizeInByte()));
                channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                if (MqttVersion.MQTT_5.equals(mqttVersion)) {
                    channelPipeline.addLast("mqtt5PacketDispatcher", mqtt5PacketDispatcher);
                } else {
                    channelPipeline.addLast("dispatcher", mqttPacketDispatcher);
                }
            }
        }));
    }

    /**
     * Support Web Socket Transport.
     * </p>
     *
     * Both WebSocket and WebSocket-Over-TLS/SSL are supported.
     * </p>
     *
     * Clients are supposed to use one of the following Server URIs to connect:
     * <ul>
     *     <li>ws://host:port/mqtt</li>
     *     <li>wss://host:port/mqtt</li>
     * </ul>
     */
    private void startWs() {
        int port = connectConf.getMqttWsPort();
        wsServerBootstrap
            .group(acceptorEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 8 * 1024)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(new InetSocketAddress(port))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(AdaptiveTlsHandler.class.getSimpleName(), adaptiveTlsHandler);
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
        LOGGER.info("MQTT server for WebSocket started, listening: {}", port);
    }

    private void startQuic() throws InterruptedException {
        ChannelHandler channelHandler = new QuicServerCodecBuilder()
            .sslContext(sslFactory.getQuicSslContext())
            .maxIdleTimeout(5000, TimeUnit.SECONDS)
            // Configure some limits for the maximal number of streams (and the data) that we want to handle.
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .initialMaxStreamsUnidirectional(100)
            .activeMigration(true)

            // Setup a token handler. In a production system you would want to implement and provide your custom
            // one.
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            // ChannelHandler that is added into QuicChannel pipeline.
            .handler(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelActive(ChannelHandlerContext ctx) {
                    QuicChannel channel = (QuicChannel) ctx.channel();
                    LOGGER.debug("QUIC Connection Established: remote={}", channel.remoteAddress());
                    // Create streams etc..
                }

                public void channelInactive(ChannelHandlerContext ctx) {
                    ((QuicChannel) ctx.channel()).collectStats().addListener(f -> {
                        if (f.isSuccess()) {
                            LOGGER.info("Connection closed: {}", f.getNow());
                        }
                    });
                }

                @Override
                public boolean isSharable() {
                    return true;
                }
            })
            .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    assembleHandlerPipeline(pipeline);
                }
            })
            .build();

        quicBootstrap.group(workerEventLoopGroup)
            .channel(NioDatagramChannel.class)
            .handler(channelHandler)
            .bind(new InetSocketAddress(connectConf.getQuicPort()))
            .sync();
        LOGGER.info("MQTT server for QUIC over DTLS started, listening: {}", connectConf.getQuicPort());
    }

    private void startCoap() {
        int port = connectConf.getCoapPort();
        coapBootstrap
                .group(new NioEventLoopGroup(connectConf.getNettyWorkerThreadNum()))
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK,new WriteBufferWaterMark(connectConf.getLowWater(), connectConf.getHighWater()))
                .localAddress(new InetSocketAddress(port))
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
//                        pipeline.addLast("coap-handler", new CoapHandler());
//                        pipeline.addLast("coap-encoder", new CoapEncoder());
//                        pipeline.addLast("coap-decoder", new CoapDecoder());
//                        pipeline.addLast("coap-dispatcher", coapPacketDispatcher);
                    }
                });
        coapBootstrap.bind();
        LOGGER.info("start coap server , port:{}", port);

    }

}
