package org.apache.rocketmq.mqtt.ds.test.meta;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.ds.meta.RetainedPersistManagerImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestRetainedPersistManagerImpl {
    void dealQueue(BlockingQueue<RetainedPersistManagerImpl.DeleteTopicTask> deleteTopicQueue, Trie<String, String>trie) throws InterruptedException {
        while (!deleteTopicQueue.isEmpty()){
            RetainedPersistManagerImpl.DeleteTopicTask take = deleteTopicQueue.take();
            long l = System.currentTimeMillis() - take.time;
            if (l<3000){
                deleteTopicQueue.put(take);
                break;
            }
            trie.deleteTrieNode(take.topic,"");
            //System.out.println(trie);
        }
    }
    @Test
    public void testDeleteTopic() throws InterruptedException {

        Trie<String, String> trie1 = new Trie<>();
        List<String> topicList1 = new ArrayList<>(Arrays.asList(
                "testTopic/t1", "testTopic/t2", "testTopic/t3", "testTopic/t1/t2", "testTopic/t1/t3", "testTopic/t1/t2/t3"));


        for (String topicFilter : topicList1) {
            // test 'addNode'
            trie1.addNode(topicFilter, "", "");
        }

        Trie<String, String> trie2 = new Trie<>();
        List<String> topicList2 = new ArrayList<>(Arrays.asList(
                "testTopic/t2", "testTopic/t3","testTopic/t1/t3"));


        for (String topicFilter : topicList2) {
            // test 'addNode'
            trie2.addNode(topicFilter, "", "");
        }

        ScheduledThreadPoolExecutor deleteTopicTrieScheduler=new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("deleteTopicKvStore"));
        BlockingQueue<RetainedPersistManagerImpl.DeleteTopicTask> deleteTopicQueue = new LinkedBlockingDeque<RetainedPersistManagerImpl.DeleteTopicTask>();
        deleteTopicQueue.add(new RetainedPersistManagerImpl.DeleteTopicTask("testTopic","testTopic/t1",System.currentTimeMillis()));
        Thread.sleep(3000);
        deleteTopicQueue.add(new RetainedPersistManagerImpl.DeleteTopicTask("testTopic","testTopic/t1/t2",System.currentTimeMillis()));
        Thread.sleep(3000);
        deleteTopicQueue.add(new RetainedPersistManagerImpl.DeleteTopicTask("testTopic","testTopic/t1/t2/t3",System.currentTimeMillis()));


        RetainedPersistManagerImpl.DeleteTopicTask take = deleteTopicQueue.take();
        take.time=System.currentTimeMillis();
        deleteTopicQueue.add(take);
        deleteTopicQueue.element();
        deleteTopicTrieScheduler.scheduleWithFixedDelay(() -> {
            try {
                dealQueue(deleteTopicQueue, trie1);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);
        Thread.sleep(15000);
        Assert.assertEquals(trie1.toString(),trie2.toString());


    }
}
