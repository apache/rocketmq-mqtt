package org.apache.rocketmq.mqtt.common.test.util;

import org.apache.rocketmq.mqtt.common.model.Trie;
import org.apache.rocketmq.mqtt.common.util.TrieUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.rocketmq.mqtt.common.util.TrieUtil.*;


public class TestTrieUtil {
    @Test
    public void test() {
        Trie<String, String> trie1 = new Trie<>();
        List<String> topicList1 = new ArrayList<>(Arrays.asList(
                "k/r/b/", "k/r/a/c/", "k/r/a/"));

        for (String topicFilter : topicList1) {
            // test 'addNode'
            trie1.addNode(topicFilter, "", "");
        }
        Trie<String, String> trie2 = new Trie<>();
        List<String> topicList2 = new ArrayList<>(Arrays.asList(
                "k/r/b/", "k/r/a/c/", "k/r/a/", "k/r/c/", "k/r/a/d/", "k/a/b/c/r/"));

        for (String topicFilter : topicList2) {
            // test 'addNode'
            trie2.addNode(topicFilter, "", "");
        }


        TrieUtil.mergeKvToLocal(trie1,trie2);
        Assert.assertEquals(trie1.toString(),trie2.toString());

        Trie<String,String>trie3= TrieUtil.rebuildLocalTrie(trie2);
        Assert.assertEquals(trie3.toString(),trie2.toString());


    }
}
