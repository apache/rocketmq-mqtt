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

package org.apache.rocketmq.mqtt.cs.starter;


import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.apache.rocketmq.mqtt.cs.protocol.rpc.RpcPacketDispatcher;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class RpcServer {
    private static Logger logger = LoggerFactory.getLogger(RpcServer.class);

    @Resource
    private ConnectConf connectConf;

    @Resource
    private RpcPacketDispatcher rpcPacketDispatcher;

    private NettyRemotingServer remotingServer;
    private BlockingQueue<Runnable> csBridgeRpcQueue;

    @PostConstruct
    public void start() {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(connectConf.getRpcListenPort());
        remotingServer = new NettyRemotingServer(nettyServerConfig);
        csBridgeRpcQueue = new LinkedBlockingQueue<>(10000);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 16, 1, TimeUnit.MINUTES,
                csBridgeRpcQueue, new ThreadFactoryImpl("Rpc_Server_Thread_"));
        remotingServer.registerDefaultProcessor(rpcPacketDispatcher, executor);
        remotingServer.start();
        logger.warn("start  rpc server , port:{}", connectConf.getRpcListenPort());
    }

}
