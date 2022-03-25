/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.mqtt.common.test;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.mqtt.common.model.Trie;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestTrie {

    @Test
    public void test() {
        Trie<String, String> trie = new Trie<>();
        List<String> topicFilterList = new ArrayList<>(Arrays.asList(
            "k/r/b/", "k/r/a/c/", "k/r/#/", "k/r/+/", "k/r/+/c/", "k/r/+/#/", "k/a/b/c/r/"));

        int index = 0;
        for (String topicFilter : topicFilterList) {
            // test 'addNode'
            trie.addNode(topicFilter, topicFilter, Integer.toString(index++));
        }

        // test 'countSubRecords'
        Assert.assertEquals(topicFilterList.size(), trie.countSubRecords());

        // test 'getNode' by 'k/r/b'
        String krbTopic = "k/r/b";
        Set<String> krbFilterSet = new HashSet<>(Arrays.asList("k/r/b/", "k/r/#/", "k/r/+/", "k/r/+/#/"));
        Set<String> krbValues = new HashSet<>(trie.getNode(krbTopic).values());
        Assert.assertEquals(krbFilterSet, krbValues);
        // test 'getNodePath'
        Set<String> krbValPaths = new HashSet<>(trie.getNodePath(krbTopic));
        Assert.assertEquals(krbFilterSet, krbValPaths);

        // test 'getNode' by 'k/r/a/c'
        String kracTopic = "k/r/a/c";
        Set<String> kracFilterSet = new HashSet<>(Arrays.asList("k/r/a/c/", "k/r/#/", "k/r/+/c/", "k/r/+/#/"));
        Set<String> kracValues = new HashSet<>(trie.getNode(kracTopic).values());
        Assert.assertEquals(kracFilterSet, kracValues);
        // test 'getNodePath'
        Set<String> kracValPaths = new HashSet<>(trie.getNodePath(kracTopic));
        Assert.assertEquals(kracFilterSet, kracValPaths);

        // test 'getNode' by 'k/r/a'
        String kraTopic = "k/r/a";
        Set<String> kraFilterSet = new HashSet<>(Arrays.asList("k/r/#/", "k/r/+/", "k/r/+/#/"));
        Set<String> kraValues = new HashSet<>(trie.getNode(kraTopic).values());
        Assert.assertEquals(kraFilterSet, kraValues);
        // test 'getNodePath'
        Set<String> kraValPaths = new HashSet<>(trie.getNodePath(kraTopic));
        Assert.assertEquals(kraFilterSet, kraValPaths);

        // test 'deleteNode' by index of 'k/a/b/c/r'
        String kabcr = topicFilterList.get(topicFilterList.size() - 1);
        Assert.assertFalse(CollectionUtils.isEmpty(trie.getNodePath(kabcr)));
        trie.deleteNode(kabcr, String.valueOf(topicFilterList.size() - 1));
        Assert.assertTrue(CollectionUtils.isEmpty(trie.getNodePath(kabcr)));

        // test 'traverseAll'
        Trie<String, String> finalTrie = trie;
        String krbPath = topicFilterList.get(0);
        trie.traverseAll((path, nodeKey) -> {
            if (krbPath.equals(path)) {
                finalTrie.deleteNode(path, String.valueOf(0));
            }
        });
        Assert.assertFalse(trie.getNodePath(krbPath).contains(krbPath));

        // test delete all trie node
        for (int i = 0; i < topicFilterList.size(); ++i) {
            trie.deleteNode(topicFilterList.get(i), String.valueOf(i));
        }
        Assert.assertTrue(MapUtils.isEmpty(trie.getNode(topicFilterList.get(0))));
    }
}