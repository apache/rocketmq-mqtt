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

package org.apache.rocketmq.mqtt.cs.protocol.ssl;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.apache.rocketmq.mqtt.cs.config.ConnectConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.net.ssl.SSLEngine;

@Component
public class SslFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SslFactory.class);

    private static final String CERT_FILE_NAME = "mqtt.crt";
    private static final String KEY_FILE_NAME = "mqtt.key";

    @Resource
    private ConnectConf connectConf;

    private SslContext sslContext;

    @PostConstruct
    private void initSslContext() {
        if (!connectConf.isEnableTlsSever()) {
            return;
        }

        try {
            InputStream certStream = new ClassPathResource(CERT_FILE_NAME).getInputStream();
            InputStream keyStream = new ClassPathResource(KEY_FILE_NAME).getInputStream();
            SslContextBuilder contextBuilder = SslContextBuilder.forServer(certStream, keyStream);
            contextBuilder.clientAuth(ClientAuth.OPTIONAL);
            contextBuilder.sslProvider(OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK);
            if (connectConf.isNeedClientAuth()) {
                LOG.info("client tls authentication is required.");
                contextBuilder.clientAuth(ClientAuth.REQUIRE);
                contextBuilder.trustManager(certStream);
            }
            sslContext = contextBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException("failed to initialize ssl context.", e);
        }
    }

    public SSLEngine buildSslEngine(SocketChannel ch) {
        SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
        sslEngine.setEnabledCipherSuites(sslEngine.getSupportedCipherSuites());
        sslEngine.setUseClientMode(false);
        sslEngine.setNeedClientAuth(connectConf.isNeedClientAuth());
        return sslEngine;
    }

}

