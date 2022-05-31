package org.apache.rocketmq.mqtt.meta.core;

import java.util.List;

import javax.annotation.Resource;

import org.apache.rocketmq.mqtt.ds.config.ServiceConf;
import org.apache.rocketmq.mqtt.meta.util.IpUtil;

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.client.DefaultRheaKVStore;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.PlacementDriverOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RheaKVStoreOptionsConfigured;

import static com.alipay.sofa.jraft.util.BytesUtil.writeUtf8;

public class Client {

    public static void main(String[] args) throws Exception {

        String allNodeAddress = "30.225.11.185:25000";
        String clusterName = "defaultCluster";
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured.newConfigured() //
            .withInitialServerList(-1L /* default id */, allNodeAddress) //
            .config();

        CliOptions cliOptions = new CliOptions();
        cliOptions.setTimeoutMs(10000);
        cliOptions.setMaxRetry(3);
        cliOptions.setRpcConnectTimeoutMs(10000);
        cliOptions.setRpcDefaultTimeout(10000);
        final PlacementDriverOptions pdOpts = PlacementDriverOptionsConfigured.newConfigured() //
            .withFake(true) //
            .withRegionRouteTableOptionsList(regionRouteTableOptionsList) //
            .withCliOptions(cliOptions)
            .config();

        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setRpcTimeoutMillis(10000);
        final RheaKVStoreOptions opts = RheaKVStoreOptionsConfigured.newConfigured() //
            .withClusterName(clusterName) //
            .withPlacementDriverOptions(pdOpts) //
            .withRpcOptions(rpcOptions)
            .withFutureTimeoutMillis(10000)
            .config();

        final RheaKVStore rheaKVStore = new DefaultRheaKVStore();
        rheaKVStore.init(opts);

        Boolean ok = rheaKVStore.bPut(writeUtf8("key"), writeUtf8("value"));
        System.out.println(ok);
        //
        //byte[] value = rheaKVStore.bGet(writeUtf8("key"));
        //System.out.println(readUtf8(value));
        //
        //ok = rheaKVStore.bCompareAndPut(writeUtf8("key"), writeUtf8("value"), writeUtf8("new_value"));
        //
        //value = rheaKVStore.bGet(writeUtf8("key"));
        //System.out.println(readUtf8(value));
        //
        //ok = rheaKVStore.bContainsKey(writeUtf8("key"));
        //System.out.println(ok);
        //
        //ok = rheaKVStore.bDelete(writeUtf8("key"));
        //System.out.println(ok);
        //
        //ok = rheaKVStore.bContainsKey(writeUtf8("key"));
        //System.out.println(ok);

        //rheaKVStore.bMerge("merge_key", "aa");
        //rheaKVStore.bMerge("merge_key", "bb");
        //rheaKVStore.bMerge("merge_key", "cc");
        //byte[] merge_value = rheaKVStore.bGet("merge_key");
        //System.out.println(readUtf8(merge_value));

        //for (int i = 0; i < 10; i++) {
        //    Sequence sequence = rheaKVStore.bGetSequence("id", 1);
        //    System.out.println(sequence.getStartValue() + "," + sequence.getEndValue());
        //}
        //Long id = rheaKVStore.bGetLatestSequence("id");
        //System.out.println(id);

        //DistributedLock<byte[]> lock = rheaKVStore.getDistributedLock("lock", 60000, TimeUnit.MILLISECONDS);
        //if (lock.tryLock()) {
        //    try {
        //    } finally {
        //        lock.unlock();
        //    }
        //} else {
        //}

        Thread.sleep(3600 * 1000);

        rheaKVStore.shutdown();
    }
}
