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

package org.apache.rocketmq.mqtt.ds.meta;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


@Component
public class SubscriptionPersistManager implements org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionPersistManager.class);
    @Resource
    private MetaClient metaClient;

    @Override
    public CompletableFuture<Set<Subscription>> loadSubscriptions(String clientId) {
        // todo caffine cache

        CompletableFuture<Set<Subscription>> subscriptions = new CompletableFuture<>();
        Set<Subscription> set = JSON.parseObject(new String(metaClient.bGet(clientId)), new TypeReference<Set<Subscription>>(){});
        subscriptions.complete(set);

//        metaClient.get(clientId).whenComplete((subs, throwable)->{
//            if(throwable != null){
//                log.error("fail to load {} subscription, reason: {}", clientId, throwable);
//            }
//            Set<Subscription> set = JSON.parseObject(new String(subs), new TypeReference<Set<Subscription>>(){});
//            subscriptions.complete(set);
//        });
        return subscriptions;
    }

    @Override
    public CompletableFuture<Set<String>> loadSubscribers(String topic) {
        // todo caffine cache

        CompletableFuture<Set<String>> subscribers = new CompletableFuture<>();
        Set<String> set = JSON.parseObject(new String(metaClient.bGet(topic)), new TypeReference<Set<String>>(){});
        subscribers.complete(set);

//        metaClient.get(topic).whenComplete((subs, throwable)->{
//            if(throwable != null){
//                log.error("fail to load {} subscribers, reason: {}", topic, throwable);
//            }
//            Set<String> set = JSON.parseObject(new String(subs), new TypeReference<Set<String>>(){});
//            subscribers.complete(set);
//        });
        return subscribers;
    }

    @Override
    public void saveSubscriptions(String clientId, Set<Subscription> subscriptions) {
        // todo caffine cache

        if(subscriptions == null || subscriptions.size() == 0){
            return;
        }

        String json = JSON.toJSONString(subscriptions);
        try{
            Boolean ok = metaClient.bPut(clientId, json.getBytes());
        }catch (Exception e){
            log.error("fail to put {}", clientId);
        }

        byte[] bytes = metaClient.bGet(clientId);
        Set<Subscription> set = JSON.parseObject(new String(bytes), new TypeReference<Set<Subscription>>() {
        });

//        metaClient.get(clientId).whenComplete((subs, throwable)->{
//            if(throwable != null){
//                log.error("fail to load {} subscription, reason: {}", clientId, throwable);
//                return;
//            }
//
//            Set<Subscription> set;
//            if(subs == null || subs.length == 0){
//                set = subscriptions;
//            }else{
//                set = JSON.parseObject(new String(subs), new TypeReference<Set<Subscription>>(){});
//                for(Subscription sub : subscriptions){
//                    set.add(sub);
//                }
//            }
//
//            String json = JSON.toJSONString(set);
//            metaClient.put(clientId, json.getBytes()).whenComplete((ok, exception)->{
//                if(!ok || exception != null){
//                    log.error("fail to save {} subscription, reason: {}", clientId, exception);
//                }
//            });
//
//
//        });

    }

    @Override
    public void saveSubscribers(String topic, Set<String> clientIds) {
        // todo caffine cache

        if(clientIds == null || clientIds.size() == 0){
            return;
        }

        metaClient.get(topic).whenComplete((subs, throwable)->{
            if(throwable != null){
                log.error("fail to load {} subscribers, reason: {}", topic, throwable);
                return;
            }

            Set<String> set;
            if(subs == null || subs.length == 0){
                set = clientIds;
            }else{
                set = JSON.parseObject(new String(subs), new TypeReference<Set<String>>(){});
                for(String clientId : clientIds){
                    set.add(clientId);
                }
            }

            String json = JSON.toJSONString(set);
            metaClient.put(topic, json.getBytes()).whenComplete((ok, exception)->{
                if(!ok || exception != null){
                    log.error("fail to save {} subscription, reason: {}", topic, exception);
                }
            });

        });
    }

    @Override
    public void removeSubscriptions(String clientId, Set<Subscription> subscriptions) {
        // todo caffine cache

        metaClient.get(clientId).whenComplete((subs, throwable)->{
            if(throwable != null){
                log.error("fail to load {} subscription, reason: {}", clientId, throwable);
                return;
            }

            if(subs == null || subs.length == 0){
                return;
            }

            Set<Subscription> set = JSON.parseObject(new String(subs), new TypeReference<Set<Subscription>>() {
            });
            int length = set.size();
            for(Subscription sub : subscriptions){
                set.remove(sub);
            }
            if (set.size() == length){
                return;
            }

            String json = JSON.toJSONString(set);
            metaClient.put(clientId, json.getBytes()).whenComplete((ok, exception)->{
                if(!ok || exception != null){
                    log.error("fail to save {} subscription, reason: {}", clientId, exception);
                }
            });


        });
    }
}
