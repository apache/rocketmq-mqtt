package org.apache.rocketmq.mqtt.ds.meta;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
import java.util.concurrent.atomic.AtomicInteger;

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
        boolean isEmptyMsg=false;
        boolean isNeedStoreMsg =false;
        if(msgPayLoad.equals(MessageUtil.EMPTYSTRING)){  //will delete retained msg
            deleteTopicQueue.add(new deleteTopicTask(firstTopic,topic,System.currentTimeMillis()));
            isEmptyMsg=true;
        }else if(localRetainedTopicTrieCache.get(firstTopic).getNodePath().size()>=LIMIT_RETAINED_MESSAGE_COUNT){
            logger.info("Local Trie reject. Put retained message of topic {} into meta failed. Because the number of topics {} exceeds the limit...", topic,firstTopic);
            isNeedStoreMsg =false;
        } else{
            isNeedStoreMsg = updateTopicTrie(firstTopic, topic);
        }

        if (isEmptyMsg|| isNeedStoreMsg){
            logger.info("isEmptyMsg={}  isNeedStoreMsg={}",isEmptyMsg, isNeedStoreMsg);
            String json = JSON.toJSONString(message, SerializerFeature.WriteClassName);
            return metaClient.put(MessageUtil.RETAINED+topic ,json.getBytes()).whenComplete((ok, exception) -> {
                if (!ok || exception != null) {
                    logger.error("Put retained message of topic {} into meta failed", topic,exception);
                }else{
                    logger.info("Put retained message of topic {} into meta success",topic);
                }
            });
        }else{
            logger.info("Put retained message of topic {} into meta failed",topic);
            result.complete(false);
            return result;
        }

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
        if(results.isEmpty()){   //Refresh the trie about single firstTopic
            logger.info("Local trie does not exist. Try to kv store find...");
            refreshSingleKVStore(firstTopic);
            results=localRetainedTopicTrieCache.get(firstTopic).getAllPath(originTopicFilter);
        }
        return results;
    }

    private boolean updateTopicTrie(String firstTopic,String topic){

        if(localRetainedTopicTrieCache.get(firstTopic).getNodePath().contains(topic)){
            return true;
        }

        Trie<String,String>localTrie=localRetainedTopicTrieCache.get(firstTopic);
        localTrie.addNode(topic,"","");

        //update the kv trie
        boolean result=false;
        while(!result){
            byte[] kvTriebytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
            Trie<String,String>kvTrie=JSON.parseObject(kvTriebytes, Trie.class);
            if (kvTrie.getNodePath().size()>=LIMIT_RETAINED_MESSAGE_COUNT){
                result=false;
                break;
            }
            TrieUtil.mergeKvToLocal(localTrie, kvTrie);  //local<-KV
            String newTrieJson = JSON.toJSONString(localTrie, SerializerFeature.WriteClassName);
            result = metaClient.bCompareAndPut(PREFIX_TOPIC_TRIE + firstTopic, kvTriebytes, newTrieJson.getBytes());  //write success
        }
        if (result) {
            logger.info("Update {} into firstTopic {} Trie successful...", topic, firstTopic);
        }else{
            logger.info("KV Trie reject. Put retained message of topic {} into meta failed. Because the number of topics {} exceeds the limit...", topic,firstTopic);
            return false;
        }
        return result;
    }

    private void refreshDeleteQueue() throws InterruptedException {
        try {
            logger.info("Start refreshDeleteQueue...");
            AtomicInteger count=new AtomicInteger(0);
            long ct=System.currentTimeMillis();
            while(!deleteTopicQueue.isEmpty()){
                if (ct-deleteTopicQueue.peek().time<LIMIT_DELETE_MESSAGE_TIME){
                    break;
                }
                deleteTopicTask take = deleteTopicQueue.take();
                execDeleteTopicFromTrie(take.firstTopic,take.topic);
                count.incrementAndGet();
            }
            logger.info("delete {} topic",count);
            logger.info("End refreshDeleteQueue...");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void execDeleteTopicFromTrie(String firstTopic,String topic){
        boolean result=false;
        while(!result){
            byte[] kvTriebytes = metaClient.bGet(PREFIX_TOPIC_TRIE + firstTopic);
            Trie<String,String>kvTrie=JSON.parseObject(kvTriebytes, Trie.class);
            kvTrie.deleteTrieNode(topic,"");
            Trie<String, String> newTrie = TrieUtil.rebuildLocalTrie(kvTrie);
            String newTrieJson = JSON.toJSONString(newTrie, SerializerFeature.WriteClassName);
            result = metaClient.bCompareAndPut(PREFIX_TOPIC_TRIE + firstTopic, kvTriebytes, newTrieJson.getBytes());  //remove topic from trie
        }
        logger.info("Delete "+firstTopic+" Trie's {} successful...",topic);

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
