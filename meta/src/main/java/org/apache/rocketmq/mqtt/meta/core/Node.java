package org.apache.rocketmq.mqtt.meta.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */
public class Node {
    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private final RheaKVStoreOptions options;
    private RheaKVStore rheaKVStore;

    public Node(RheaKVStoreOptions options) {
        this.options = options;
    }

    public boolean start() {
        this.rheaKVStore = new DefaultRheaKVStore();
        return this.rheaKVStore.init(this.options);
    }

    public void stop() {
        this.rheaKVStore.shutdown();
    }

    public RheaKVStore getRheaKVStore() {
        return rheaKVStore;
    }
}
