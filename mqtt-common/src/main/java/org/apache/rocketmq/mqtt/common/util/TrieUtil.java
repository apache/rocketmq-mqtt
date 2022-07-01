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


package org.apache.rocketmq.mqtt.common.util;

import org.apache.rocketmq.mqtt.common.model.Trie;

import java.util.HashSet;
import java.util.Set;

public class TrieUtil {

    public static void mergeKvToLocal(Trie<String, String> localTrie, Trie<String, String> kvTrie) {
        Set<String> localTrieNodePath = localTrie.getNodePath();
        Set<String> kvTrieNodePath = kvTrie.getNodePath();
        Set<String> result = new HashSet<>(kvTrieNodePath);
        result.removeAll(localTrieNodePath);
        for (String topic : result) {
            localTrie.addNode(topic, "", "");
        }
    }

    public static Trie<String, String> rebuildLocalTrie(Trie<String, String> kvTrie) {
        Set<String> kvTrieNodePath = kvTrie.getNodePath();
        Set<String> result = new HashSet<>(kvTrieNodePath);
        Trie<String, String> localTrie = new Trie<>();
        for (String topic : result) {
            localTrie.addNode(topic, "", "");
        }
        return localTrie;
    }
}

