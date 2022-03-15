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

package org.apache.rocketmq.mqtt.cs.session.infly;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class MqttMsgId {

    private static final int MAX_MSG_ID = 65535;
    private static final int MIN_MSG_ID = 1;

    private static final int ID_POOL_SIZE = 8192;
    private static final List<MsgIdEntry> ID_POOL = new ArrayList<>(ID_POOL_SIZE);

    @PostConstruct
    public void init() {
        for (int i = 0; i < ID_POOL_SIZE; i++) {
            ID_POOL.add(new MsgIdEntry());
        }
    }

    class MsgIdEntry {
        private int nextMsgId = MIN_MSG_ID - 1;
        private Map<Integer, Integer> inUseMsgIds = new ConcurrentHashMap<>();
    }

    private MsgIdEntry hashMsgID(String clientId) {
        int hashCode = clientId.hashCode();
        if (hashCode < 0) {
            hashCode *= -1;
        }
        return ID_POOL.get(hashCode % ID_POOL_SIZE);
    }
 
    public int nextId(String clientId) {
        MsgIdEntry msgIdEntry = hashMsgID(clientId);
        synchronized (msgIdEntry) {
            int startingMessageId = msgIdEntry.nextMsgId;
            int loopCount = 0;
            int maxLoopCount = 1;
            do {
                msgIdEntry.nextMsgId++;
                if (msgIdEntry.nextMsgId > MAX_MSG_ID) {
                    msgIdEntry.nextMsgId = MIN_MSG_ID;
                }
                if (msgIdEntry.nextMsgId == startingMessageId) {
                    loopCount++;
                    if (loopCount >= maxLoopCount) {
                        msgIdEntry.inUseMsgIds.clear();
                        break;
                    }
                }
            } while (msgIdEntry.inUseMsgIds.containsKey(new Integer(msgIdEntry.nextMsgId)));
            Integer id = new Integer(msgIdEntry.nextMsgId);
            msgIdEntry.inUseMsgIds.put(id, id);
            return msgIdEntry.nextMsgId;
        }
    }

    public void releaseId(int msgId, String clientId) {
        if (StringUtils.isBlank(clientId)) {
            return;
        }
        MsgIdEntry msgIdEntry = hashMsgID(clientId);
        msgIdEntry.inUseMsgIds.remove(msgId);
    }

}
