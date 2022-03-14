## Apache RocketMQ MQTT
[![Build Status](https://api.travis-ci.com/apache/rocketmq-mqtt.svg?branch=main)](https://travis-ci.com/github/apache/rocketmq-mqtt)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq-mqtt)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-mqtt.svg)](http://isitmaintained.com/project/apache/rocketmq-mqtt "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-mqtt.svg)](http://isitmaintained.com/project/apache/rocketmq-mqtt "Percentage of issues still open")

A new MQTT protocol architecture model, based on which RocketMQ can better support messages from terminals such as IoT devices and Mobile APP. Based on the RocketMQ message unified storage engine, it supports both MQTT terminal and server message sending and receiving.

## Architecture
The relevant architecture design is introduced in [RIP-33](https://docs.google.com/document/d/1AD1GkV9mqE_YFA97uVem4SmB8ZJSXiJZvzt7-K6Jons/edit#).
The queue model of MQTT needs to be based on the light message queue, For the design of the specific light message queue, please refer to the [RIP-28(LMQ)-wiki](https://github.com/apache/rocketmq/wiki/RIP-28-Light-message-queue-(LMQ) of [RocketMQ](https://github.com/apache/rocketmq).

## Get Started

### Prerequisites
The queue model of MQTT needs to be based on the light message queue feature ([RIP-28](https://github.com/apache/rocketmq/pull/3694)) of RocketMQ. RocketMQ has only supported this feature since version 4.9.3. Please ensure that the installed version of RocketMQ already supports this feature.

For the quick start of light message queues, please refer to [Example_LMQ](https://github.com/apache/rocketmq/blob/develop/docs/cn/Example_LMQ.md) of RocketMQ. 


1. Clone
```shell
git clone https://github.com/apache/rocketmq-mqtt
```
2. Build the package
```shell
cd rocketmq-mqtt
mvn clean package -DskipTests=true assembly:assembly
```
3. Config
```shell
cp -r  target/rocketmq-mqtt ~
cd ~/rocketmq-mqtt
cd conf
```
Some important configuration items in the **service.conf** configuration file

**Config Key** | **Instruction**
----- | ----  
username   |  used for auth
secretKey   | used for auth
NAMESRV_ADDR   | specify namesrv address
eventNotifyRetryTopic   | notify event retry topic
clientRetryTopic   | client retry topic

4. CreateTopic

   create all first-level topics, including **eventNotifyRetryTopic** and **clientRetryTopic** in the configuration file above.
```shell
sh mqadmin updatetopic -c {cluster} -t {topic} -n {namesrv}
```
5. Initialize Meta
- Configure Gateway Node List
```shell
sh mqadmin updateKvConfig -s LMQ -k LMQ_CONNECT_NODES -v {ip1,ip2} -n {namesrv}
```
- Configure the first-level topic list
```shell
sh mqadmin updateKvConfig -s LMQ -k ALL_FIRST_TOPICS -v {topic1,topic2} -n {namesrv}
```
- Configure a list of wildcard characters under each first-level topic
```shell
sh mqadmin updateKvConfig  -s LMQ -k {topic} -v {topic/+}  -n {namesrv}
```
6. Start Process
```shell
cd bin
sh mqtt.sh start
```
### Example
The mqtt-example module has written basic usage example code, which can be used for reference

## Protocol Version
The currently supported protocol version is [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf), but the will and retain features are not supported yet

## Authentication
At present, an implementation based on the HmacSHA1 signature algorithm is provided by default, Refer to **AuthManagerSample**. Users can customize other implementations to meet the needs of businesses to flexibly verify resources and identities.
## Meta Persistence
At present, meta data storage and management is simply implemented through the kvconfig mechanism of namesrv by default, Refer to **MetaPersistManagerSample**. Users can customize other implementations.

## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation.
