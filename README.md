## Apache RocketMQ MQTT
[![Build Status](https://api.travis-ci.com/apache/rocketmq-mqtt.svg?branch=main)](https://travis-ci.com/github/apache/rocketmq-mqtt)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-mqtt/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq-mqtt)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-mqtt.svg)](http://isitmaintained.com/project/apache/rocketmq-mqtt "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-mqtt.svg)](http://isitmaintained.com/project/apache/rocketmq-mqtt "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)

A new MQTT protocol architecture model, based on which RocketMQ can better support messages from terminals such as IoT devices and Mobile APP. Based on the RocketMQ message unified storage engine, it supports both MQTT terminal and server message sending and receiving.The entire project design refer to [MQTT overview](https://rocketmq.apache.org/docs/mqtt/01RocketMQMQTTOverview).

## Architecture
The relevant architecture design is introduced in [RIP-33](https://docs.google.com/document/d/1AD1GkV9mqE_YFA97uVem4SmB8ZJSXiJZvzt7-K6Jons/edit#).
The queue model of MQTT needs to be based on the light message queue, For the design of the specific light message queue, please refer to the [RIP-28(LMQ)-wiki](https://github.com/apache/rocketmq/wiki/RIP-28-Light-message-queue-%28LMQ%29) of [RocketMQ](https://github.com/apache/rocketmq).

## Get Started

### Prerequisites
The queue model of MQTT needs to be based on the light message queue feature ([RIP-28](https://github.com/apache/rocketmq/pull/3694)) of RocketMQ. RocketMQ has only supported this feature since version 4.9.3. Please ensure that the installed version of RocketMQ already supports this feature.

For the quick start of light message queue, please refer to [Example_LMQ](https://github.com/apache/rocketmq/blob/develop/docs/cn/Example_LMQ.md) of RocketMQ. 
For example, set the following parameters to true in broker.conf
```
enableLmq = true
enableMultiDispatch = true
```
### Build Requirements
The current project requires JDK 1.8.x.  When building on MAC arm64 the recommended JDK 1.8 must be based on 386 architecture or use Maven flag `-Dos.arch=x86_64` when building with Maven.


1. Clone
```shell
git clone https://github.com/apache/rocketmq-mqtt
```
2. Build the package
```shell
cd rocketmq-mqtt
mvn -Prelease-all -DskipTests clean install -U 
```
For building on Apple M1/M2, please use the following command:
```shell
mvn -Prelease-all -DskipTests clean install -U -Dos.detected.classifier=osx-x86_64
```
or specify the profile in the `~/.m2/settings.xml` file:
```xml
<profiles>
    <profile>
        <id>osx-x86_64</id>
        <activation>
            <os>
                <name>osx</name>
                <arch>x86_64</arch>
            </os>
        </activation>
        <properties>
            <os.detected.classifier>osx-x86_64</os.detected.classifier>
        </properties>
    </profile>
</profiles>
```
3. Config
```shell
cd distribution/target/
cd conf
```
Some important configuration items in the **service.conf** configuration file 

| **Config Key**        | **Instruction**                                               |
|-----------------------|---------------------------------------------------------------|
| username              | used for auth                                                 |
| secretKey             | used for auth                                                 |
| NAMESRV_ADDR          | specify namesrv address                                       |
| eventNotifyRetryTopic | notify event retry topic                                      |
| clientRetryTopic      | client retry topic                                            |
| metaAddr              | meta all nodes ip:port. Same as membersAddress in meta.config |


And some configuration items in the **meta.conf** configuration file

| **Config Key** | **Instruction**                                                                 |
|----------------|---------------------------------------------------------------------------------|
| selfAddress    | meta cur node ip:port, e.g. 192.168.0.1:8080                                    |
| membersAddress | meta all nodes ip:port, e.g. 192.168.0.1:8080,192.168.0.2:8080,192.168.0.3:8080 |

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
sh meta.sh start
sh mqtt.sh start
```
### Example
The mqtt-example module has written basic usage example code, which can be used for reference

## Protocol Version
The currently supported protocol version is [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf).

## Authentication
At present, an implementation based on the HmacSHA1 signature algorithm is provided by default, Refer to **AuthManagerSample**. Users can customize other implementations to meet the needs of businesses to flexibly verify resources and identities.
## Meta Persistence
At present, meta data storage and management is simply implemented through the kvconfig mechanism of namesrv by default, Refer to **MetaPersistManagerSample**. Users can customize other implementations.

## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation.
