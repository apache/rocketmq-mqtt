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

package org.apache.rocketmq.mqtt.meta.core;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.rocketmq.mqtt.meta.config.MetaConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import org.apache.rocketmq.mqtt.meta.util.IpUtil;

import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RocksDBOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.StoreEngineOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.util.Endpoint;

@Service
public class MetaServer {
    private static final Logger log = LoggerFactory.getLogger(MetaServer.class);

    @Resource
    private MetaConf metaConf;

    @PostConstruct
    public void start() {

        String ip = IpUtil.getLocalAddressCompatible();
        String allNodeAddress = IpUtil.convertAllNodeAddress(metaConf.getAllNodeAddress(),
            metaConf.getMetaPort());

        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured.newConfigured()
            .withInitialServerList(-1L /* default id */, allNodeAddress)
            .config();

        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured()
            .withFake(true)
            .withRegionRouteTableOptionsList(regionRouteTableOptionsList)
            .config();

        final RocksDBOptions rocksDBOptions = RocksDBOptionsConfigured.newConfigured().
            withDbPath(metaConf.getDbPath()).
            config();

        final StoreEngineOptions storeOpts = StoreEngineOptionsConfigured.newConfigured()
            .withStorageType(StorageType.RocksDB)
            .withRocksDBOptions(rocksDBOptions)
            .withRaftDataPath(metaConf.getRaftDataPath())
            .withServerAddress(new Endpoint(ip, metaConf.getMetaPort()))
            .config();

        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured()
            .withClusterName(metaConf.getClusterName())
            .withInitialServerList(allNodeAddress)
            .withUseParallelCompress(true)
            .withStoreEngineOptions(storeOpts)
            .withPlacementDriverOptions(pdOpts)
            .config();

        final Node node = new Node(opts);
        log.info("create meta node, node config: {}", opts);

        if (node.start()) {
            Runtime.getRuntime().addShutdownHook(new Thread(node::stop));
            System.out.println("server start ok. " + ip + ":" + metaConf.getMetaPort());
        } else {
            System.out.println("server start fail. " + ip + ":" + metaConf.getMetaPort());
        }
    }
}
