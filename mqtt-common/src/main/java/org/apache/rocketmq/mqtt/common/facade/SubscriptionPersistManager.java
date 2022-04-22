package org.apache.rocketmq.mqtt.common.facade;

import org.apache.rocketmq.mqtt.common.model.Subscription;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriptionPersistManager {
    /**
     * loadSubscriptions
     *
     * @param clientId
     * @return
     */
    CompletableFuture<Set<Subscription>> loadSubscriptions(String clientId);

    /**
     * saveSubscriptions
     *
     * @param clientId
     * @param subscriptions
     */
    void saveSubscriptions(String clientId, Set<Subscription> subscriptions);

    /**
     * removeSubscriptions
     *
     * @param clientId
     * @param subscriptions
     */
    void removeSubscriptions(String clientId, Set<Subscription> subscriptions);
}
