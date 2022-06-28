package org.apache.rocketmq.mqtt.common.facade;

import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.Trie;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface RetainedPersistManager {

    /**
     * get wildcards of the first topic
     * @param firstTopic
     * @return
     */
    Trie<String,String>getTries(String firstTopic);

    CompletableFuture<Boolean> storeRetainedMessage(String topic, Message message);

    CompletableFuture<byte[]>getRetainedMessage(String preciseTopic);

    Set<String> getTopicsFromTrie(Subscription topicFilter);
}
