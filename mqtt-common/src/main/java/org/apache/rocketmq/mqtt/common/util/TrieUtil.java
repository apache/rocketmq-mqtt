package org.apache.rocketmq.mqtt.common.util;

import org.apache.rocketmq.mqtt.common.model.Trie;

import java.util.HashSet;
import java.util.Set;

public class TrieUtil {

    public static void mergeKvToLocal(Trie<String, String> localTrie, Trie<String, String> kvTrie){
        Set<String> localTrieNodePath = localTrie.getNodePath();
        Set<String> kvTrieNodePath = kvTrie.getNodePath();
        Set<String> result = new HashSet<>(kvTrieNodePath);
        result.removeAll(localTrieNodePath);
        for (String topic:result){
            localTrie.addNode(topic,"","");
        }
    }
    public static Trie<String,String> rebuildLocalTrie(Trie<String,String>kvTrie){
        Set<String> kvTrieNodePath = kvTrie.getNodePath();
        Set<String> result = new HashSet<>(kvTrieNodePath);
        Trie<String,String>localTrie=new Trie<>();
        for (String topic:result){
            localTrie.addNode(topic,"","");
        }
        return localTrie;
    }
}

