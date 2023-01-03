package org.apache.rocketmq.mqtt.common.facade;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface WillMsgPersistManager {

    CompletableFuture<Boolean> put(final String key, final String value);
    CompletableFuture<Boolean> delete(final String key);

    CompletableFuture<byte[]> get(final String key);

    CompletableFuture<Boolean> compareAndPut(final String key, final String expectValue, final String updateValue);

    CompletableFuture<Map<String, String>> scan(final String startKey, final String endKey);
}
