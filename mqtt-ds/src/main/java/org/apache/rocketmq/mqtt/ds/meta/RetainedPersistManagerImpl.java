package org.apache.rocketmq.mqtt.ds.meta;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alipay.sofa.jraft.rhea.util.concurrent.DistributedLock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.MetaPersistManager;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.Subscription;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.common.util.TrieUtil;
import org.apache.rocketmq.mqtt.meta.core.MetaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;

public class RetainedPersistManagerImpl implements RetainedPersistManager {

    private static Logger logger = LoggerFactory.getLogger(RetainedPersistManagerImpl.class);

    private volatile Map<String, Trie<String,String> > localRetainedTopicTrieCache = new ConcurrentHashMap<>();

    private ScheduledThreadPoolExecutor refreshScheduler;  //ThreadPool to refresh local Trie

    private ScheduledThreadPoolExecutor deleteTopicTrieScheduler;  //ThreadPool to delete topic

    BlockingQueue<deleteTopicTask> deleteTopicQueue = new LinkedBlockingDeque<deleteTopicTask>();
    @Resource
    private MetaPersistManager metaPersistManager;
    @Resource
    private MetaClient metaClient;

    private static final String PREFIX_TOPIC_TRIE = "%RETAINED_TOPIC_TRIE%";

    private static final String LOCK_TOPIC_TRIE= "%LOCK_TOPIC_TRIE%";

    private final int LIMIT_RETAINED_MESSAGE_COUNT=100;

    private final long LIMIT_DELETE_MESSAGE_TIME=3000;

    public void init(){
        refreshScheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("refreshKVStore"));
        refreshScheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshKVStore();
            } catch (Throwable t) {
                logger.error("", t);
            }
        }, 3, 3, TimeUnit.SECONDS);

        deleteTopicTrieScheduler=new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("deleteTopicKvStore"));
        deleteTopicTrieScheduler.scheduleWithFixedDelay(() -> {
            try {
                refreshDeleteQueue();
            } catch (Throwable t) {
                logger.error("", t);
            }
        }, 5, 3, TimeUnit.SECONDS);
    }


    @Override
    public Trie<String, String> getTries(String firstTopic) {
        Set<String> allFirstTopics = metaPersistManager.getAllFirstTopics();
        if (!allFirstTopics.contains(firstTopic)){
            return null;
        }else{
            return localRetainedTopicTrieCache.get(firstTopic);
        }
    }

    private void refreshKVStore(){ //Refresh all firstTopic
        long start = System.currentTimeMillis();
        logger.info(" Start refresh the tries...");
        Set<String> allFirstTopics = metaPersistManager.getAllFirstTopics();
        for (String firstTopic : allFirstTopics){
            //If local Trie is null
            if (localRetainedTopicTrieCache.get(firstTopic)==null){
                localRetainedTopicTrieCache.put(firstTopic,new Trie<String,String>());
            }
            CompletableFuture<byte[]> completableFuture = metaClient.get(PREFIX_TOPIC_TRIE + firstTopic);
            completableFuture.whenComplete((trieBytes, throwable)->{
                if (trieBytes==null){  // init trie
                    Trie<String,String>trie=new Trie<String,String>();
                    String json = JSON.toJSONString(trie, SerializerFeature.WriteClassName);
                    CompletableFuture<Boolean> putFuture = metaClient.put(PREFIX_TOPIC_TRIE + firstTopic, json.getBytes());
                    putFuture.whenComplete((resultBytes,resultThrowable)->{
                        if (resultBytes==null)
                            logger.info("Init the "+ firstTopic +" trie successful...");
                    });
                }else{
                    Trie<String, String> kvTrie = JSON.parseObject(metaClient.bGet(PREFIX_TOPIC_TRIE+ firstTopic), Trie.class);
                    logger.info("The firstTopic {} kvTrie: {}",firstTopic,kvTrie.toString());
                    Trie<String, String> localTrie = TrieUtil.rebuildLocalTrie(kvTrie);
                    TrieUtil.mergeKvToLocal(localTrie, kvTrie);//local<-kvTrie
                    localRetainedTopicTrieCache.put(firstTopic,localTrie);
                }
            });
        }
        logger.info("Refresh the tries cost rt {}",System.currentTimeMillis()-start);
    }

    private void refreshSingleKVStore(String firstTopic){   //Refresh single firstTopic
        byte[] bytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
        Trie<String, String> kvTrie = JSON.parseObject(metaClient.bGet(PREFIX_TOPIC_TRIE+ firstTopic), Trie.class);
        Trie<String, String> localTrie = TrieUtil.rebuildLocalTrie(kvTrie);
        TrieUtil.mergeKvToLocal(localTrie, kvTrie);//local<-kvTrie
        localRetainedTopicTrieCache.put(firstTopic,localTrie);
    }

    public CompletableFuture<Boolean> storeRetainedMessage(String topic, Message message){
        CompletableFuture<Boolean>result = new CompletableFuture<>();
        String firstTopic;
        int index = topic.indexOf(Constants.MQTT_TOPIC_DELIMITER, 1);
        if (index > 0) {
            firstTopic = topic.substring(0, index);
        } else {
            firstTopic = topic;
        }

        if (!metaPersistManager.getAllFirstTopics().contains(firstTopic)){
            logger.info("Put retained message of topic {} into meta failed. Because first topic {} does not exist...", topic,firstTopic);
            result.complete(false);
            return result;
        }

        String msgPayLoad = new String(message.getPayload());
        byte[] kvTriebytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
        Trie<String,String>kvTrie=JSON.parseObject(kvTriebytes, Trie.class);
        if (kvTrie.getNodeCount()>LIMIT_RETAINED_MESSAGE_COUNT&&!msgPayLoad.equals(MessageUtil.EMPTYSTRING)){
            logger.info("Put retained message of topic {} into meta failed. Because the number of topics {} exceeds the limit...", topic,firstTopic);
            result.complete(false);
            return result;
        }

        String json = JSON.toJSONString(message, SerializerFeature.WriteClassName);
        return metaClient.put(MessageUtil.RETAINED+topic ,json.getBytes()).whenComplete((ok, exception) -> {
            if (!ok || exception != null) {
                logger.error("Put retained message of topic {} into meta failed", topic,exception);
            }else{
                logger.info("Put retained message of topic {} into meta success",topic);
                updateTopicTrie(firstTopic,topic);
                if (msgPayLoad.equals(MessageUtil.EMPTYSTRING)){
                    deleteTopicQueue.add(new deleteTopicTask(firstTopic,topic,System.currentTimeMillis()));
                }
            }
        });
    }
    public CompletableFuture<byte[]> getRetainedMessage(String preciseTopic){  //precise preciseTopic
        String queryKey = MessageUtil.RETAINED + TopicUtils.normalizeTopic(preciseTopic);
        return metaClient.get(queryKey);
    }

    public Set<String> getTopicsFromTrie(Subscription subscription){
        Set<String>results=new HashSet<>();
        String firstTopic=subscription.toFirstTopic();
        String originTopicFilter=subscription.getTopicFilter();
        logger.info("firstTopic={}   originTopicFilter={}",firstTopic,originTopicFilter);
        results=localRetainedTopicTrieCache.get(firstTopic).getAllPath(originTopicFilter);
        if(results.isEmpty()){   //Refresh the trie about firstTopic
            logger.info("Local trie does not exist. Try to kv store find...");
            refreshSingleKVStore(firstTopic);
            results=localRetainedTopicTrieCache.get(firstTopic).getAllPath(originTopicFilter);
        }
        return results;
    }

    private void updateTopicTrie(String firstTopic,String topic){

        if(localRetainedTopicTrieCache.get(firstTopic).getNodePath().contains(topic)){
            return;
        }

        Trie<String,String>localTrie=localRetainedTopicTrieCache.get(firstTopic);
        localTrie.addNode(topic,"","");
        //update the kv trie
        DistributedLock<byte[]> trieLock = metaClient.getDistributedLock(LOCK_TOPIC_TRIE+ firstTopic,3,TimeUnit.SECONDS);
        if (trieLock.tryLock()) {
            try {
                logger.info("Get the lock of "+firstTopic+" for update successful...");
                // manipulate protected state
                logger.info("Get {} kvTrie ...",firstTopic);
                byte[] kvTriebytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
                Trie<String,String>kvTrie=JSON.parseObject(kvTriebytes, Trie.class);
                TrieUtil.mergeKvToLocal(localTrie, kvTrie);  //local<-KV
                String newTrieJson = JSON.toJSONString(localTrie, SerializerFeature.WriteClassName);
                boolean ok = metaClient.bCompareAndPut(PREFIX_TOPIC_TRIE + firstTopic, kvTriebytes, newTrieJson.getBytes());
                if (ok){
                    logger.info("Update "+firstTopic+" Trie successful...");
                }else{
                    logger.info("Update "+firstTopic+" Trie failed...");
                }
            } finally {
                trieLock.unlock();
                logger.info("Unlock the lock of "+firstTopic+" successful...");
            }
        } else {
            // perform alternative actions
        }
    }

    private void refreshDeleteQueue() throws InterruptedException {
        try {
            logger.info("Start refreshDeleteQueue...");
            int count=0;
            long ct=System.currentTimeMillis();
            while(!deleteTopicQueue.isEmpty()){
                if (ct-deleteTopicQueue.peek().time<LIMIT_DELETE_MESSAGE_TIME){
                    break;
                }
                deleteTopicTask take = deleteTopicQueue.take();
                execDeleteTopicFromTrie(take.firstTopic,take.topic);
                count++;
            }
            logger.info("delete {} topic",count);
            logger.info("End refreshDeleteQueue...");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void execDeleteTopicFromTrie(String firstTopic,String topic){
        DistributedLock<byte[]> trieLock = metaClient.getDistributedLock(LOCK_TOPIC_TRIE+ firstTopic,3,TimeUnit.SECONDS);
        if (trieLock.tryLock()) {
            try {
                logger.info("Get the lock of "+firstTopic+"for delete successful...");
                // manipulate protected state
                logger.info("Get {} kvTrie ...",firstTopic);
                byte[] kvTriebytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
                Trie<String,String>kvTrie=JSON.parseObject(metaClient.bGet(PREFIX_TOPIC_TRIE+firstTopic), Trie.class);
                kvTrie.deleteTrieNode(topic,"");
                Trie<String, String> newTrie = TrieUtil.rebuildLocalTrie(kvTrie);
                String newTrieJson = JSON.toJSONString(newTrie, SerializerFeature.WriteClassName);
                boolean ok = metaClient.bCompareAndPut(PREFIX_TOPIC_TRIE + firstTopic, kvTriebytes, newTrieJson.getBytes());
                if (ok){
                    logger.info("Delete "+firstTopic+" Trie's {} successful...",topic);
                }else{
                    logger.info("Delete "+firstTopic+" Trie's {} failed...",topic);
                }
            } finally {
                trieLock.unlock();
                logger.info("Unlock the lock of "+firstTopic+" successful...");
            }
        } else {
            // perform alternative actions
        }
    }
    public static class deleteTopicTask {
        public String firstTopic;
        public String topic;

        public long time;
        public deleteTopicTask(String firstTopic, String topic,long time){
            this.firstTopic=firstTopic;
            this.topic=topic;
            this.time=time;
        }

        @Override
        public String toString() {
            return this.firstTopic+" "+this.topic+" "+this.time;
        }
    }


}
