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

package org.apache.rocketmq.mqtt.common.model;


public class MqttTopic {
    private String firstTopic;
    private String secondTopic;

    public MqttTopic(String firstTopic, String secondTopic) {
        this.firstTopic = firstTopic;
        this.secondTopic = secondTopic;
    }

    public String getFirstTopic() {
        return firstTopic;
    }

    public void setFirstTopic(String firstTopic) {
        this.firstTopic = firstTopic;
    }

    public String getSecondTopic() {
        return secondTopic;
    }

    public void setSecondTopic(String secondTopic) {
        this.secondTopic = secondTopic;
    }
}
