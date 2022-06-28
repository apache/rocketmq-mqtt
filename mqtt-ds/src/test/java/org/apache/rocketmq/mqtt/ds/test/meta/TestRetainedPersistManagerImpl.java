package org.apache.rocketmq.mqtt.ds.test.meta;

import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.ds.meta.RetainedPersistManagerImpl;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestRetainedPersistManagerImpl {
    void dealQueue(BlockingQueue<RetainedPersistManagerImpl.deleteTopicTask> deleteTopicQueue) throws InterruptedException {
        while (!deleteTopicQueue.isEmpty()){
            RetainedPersistManagerImpl.deleteTopicTask take = deleteTopicQueue.take();
            long l = System.currentTimeMillis() - take.time;
            System.out.println(l);
            if (l<3000){
                deleteTopicQueue.put(take);
                break;
            }

        }
    }
    @Test
    public void test() throws InterruptedException {
        ScheduledThreadPoolExecutor deleteTopicTrieScheduler=new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("deleteTopicKvStore"));
        BlockingQueue<RetainedPersistManagerImpl.deleteTopicTask> deleteTopicQueue = new LinkedBlockingDeque<RetainedPersistManagerImpl.deleteTopicTask>();
        deleteTopicQueue.add(new RetainedPersistManagerImpl.deleteTopicTask("yutao-f1/t1","yutao-f1",System.currentTimeMillis()));
        //Thread.sleep(3000);
        deleteTopicQueue.add(new RetainedPersistManagerImpl.deleteTopicTask("yutao-f1/t2","yutao-f1",System.currentTimeMillis()));
        //Thread.sleep(3000);
        deleteTopicQueue.add(new RetainedPersistManagerImpl.deleteTopicTask("yutao-f1/t3","yutao-f1",System.currentTimeMillis()));


        RetainedPersistManagerImpl.deleteTopicTask take = deleteTopicQueue.take();
        take.time=System.currentTimeMillis();
        deleteTopicQueue.add(take);
        deleteTopicQueue.element();
        deleteTopicTrieScheduler.scheduleWithFixedDelay(() -> {
            try {
                dealQueue(deleteTopicQueue);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);
        Thread.sleep(100000);
    }
}
