package org.apache.rocketmq.mqtt.ds.meta;

import org.apache.rocketmq.mqtt.common.facade.WillMsgPersistManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class WillMsgPersistManagerImpl implements WillMsgPersistManager {
    private static Logger logger = LoggerFactory.getLogger(WillMsgPersistManagerImpl.class);

    @Resource
    private WillMsgClient willMsgClient;

    @Override
    public CompletableFuture<Boolean> put(String key, String value) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            willMsgClient.put(key, value, future);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            logger.error("", e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> delete(String key) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            willMsgClient.delete(key, future);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            logger.error("", e);
        }

        return future;
    }

    @Override
    public CompletableFuture<byte[]> get(String key) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        try {
            willMsgClient.get(key, future);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            logger.error("", e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Boolean> compareAndPut(String key, String compareAndPut, String updateValue) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            willMsgClient.compareAndPut(key, compareAndPut, updateValue, future);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            logger.error("", e);
        }

        return future;
    }

    @Override
    public CompletableFuture<Map<String, String>> scan(String startKey, String endKey) {
        CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        try {
            willMsgClient.scan(startKey, endKey, future);
            return future;
        } catch (Exception e) {
            future.completeExceptionally(e);
            logger.error("", e);
        }

        return future;
    }


}
