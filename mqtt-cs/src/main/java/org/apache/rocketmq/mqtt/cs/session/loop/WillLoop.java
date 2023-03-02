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

package org.apache.rocketmq.mqtt.cs.session.loop;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.mqtt.common.facade.WillMsgPersistManager;
import org.apache.rocketmq.mqtt.common.facade.WillMsgSender;
import org.apache.rocketmq.mqtt.common.meta.IpUtil;
import org.apache.rocketmq.mqtt.common.model.Constants;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.model.WillMessage;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.cs.channel.ChannelInfo;
import org.apache.rocketmq.mqtt.cs.session.infly.MqttMsgId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
public class WillLoop {
    private static Logger logger = LoggerFactory.getLogger(WillLoop.class);
    private ScheduledThreadPoolExecutor aliveService = new ScheduledThreadPoolExecutor(2, new ThreadFactoryImpl("check_alive_thread_"));
    private long checkAliveIntervalMillis = 5 * 1000;
    private ThreadPoolExecutor executor;
    private boolean enableLoop = true;

    @Resource
    private WillMsgPersistManager willMsgPersistManager;

    @Resource
    private MqttMsgId mqttMsgId;

    @Resource
    private WillMsgSender willMsgSender;

    @PostConstruct
    public void init() {
        aliveService.scheduleWithFixedDelay(() -> csLoop(), 15 * 1000, 10 * 1000, TimeUnit.MILLISECONDS);
        aliveService.scheduleWithFixedDelay(() -> masterLoop(), 10 * 1000, 10 * 1000, TimeUnit.MILLISECONDS);

        executor = new ThreadPoolExecutor(
                1,
                1,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(5000),
                new ThreadFactoryImpl("DispatchWillToMQ_ "));
    }

    private void csLoop() {
        try {
            if (!enableLoop) {
                return;
            }
            String ip = IpUtil.getLocalAddressCompatible();
            String csKey = wrapAliveCsKeyPrefix() + Constants.CTRL_1 + ip;
            String masterKey = wrapMasterKey();
            long currentTime = System.currentTimeMillis();

            willMsgPersistManager.put(csKey, String.valueOf(currentTime)).whenComplete((result, throwable) -> {
                if (result == null || throwable != null) {
                    logger.error("{} fail to put csKey", csKey, throwable);
                }
            });

            willMsgPersistManager.get(masterKey).whenComplete((result, throwable) -> {
                String content = new String(result);
                if (Constants.NOT_FOUND.equals(content) || masterHasDown(content)) {
                    willMsgPersistManager.compareAndPut(masterKey, content, ip + Constants.COLON + currentTime).whenComplete((rs, tb) -> {
                        if (!rs || tb != null) {
                            logger.error("{} fail to update master", ip, tb);
                            return;
                        }
                        logger.info("{} update master successfully", ip);
                    });
                }
            });
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    protected String wrapMasterKey() {
        return Constants.CS_MASTER;
    }

    protected String wrapAliveCsKeyPrefix() {
        return Constants.CS_ALIVE;
    }

    private boolean masterHasDown(String masterValue) {
        String[] ipTime = masterValue.split(Constants.COLON);
        if (ipTime.length < 2) {
            logger.error("master ip:updateTime split error, len < 2");
            return true;
        }

        return System.currentTimeMillis() - Long.parseLong(ipTime[1]) > 10 * checkAliveIntervalMillis;
    }

    private void masterLoop() {
        try {
            if (!enableLoop) {
                return;
            }
            String ip = IpUtil.getLocalAddressCompatible();
            if (ip == null) {
                logger.error("can not get local ip");
                return;
            }

            willMsgPersistManager.get(wrapMasterKey()).whenComplete((result, throwable) -> {
                if (result == null || throwable != null) {
                    logger.error("fail to get CS_MASTER", throwable);
                    return;
                }

                String content = new String(result);
                if (Constants.NOT_FOUND.equals(content)) {
                    // no master
                    return;
                }

                if (!content.startsWith(ip)) {
                    // is not master
                    return;
                }
                // master keep alive
                long currentTime = System.currentTimeMillis();
                willMsgPersistManager.compareAndPut(wrapMasterKey(), content, ip + Constants.COLON + currentTime).whenComplete((rs, tb) -> {
                    if (!rs || tb != null) {
                        logger.error("{} fail to update master", ip, tb);
                    }
                });

                // master to check all cs state
                String startCSKey = wrapAliveCsKeyPrefix() + Constants.CTRL_0;
                String endCSKey = wrapAliveCsKeyPrefix() + Constants.CTRL_2;
                willMsgPersistManager.scan(startCSKey, endCSKey).whenComplete((rs, tb) -> {
                    if (rs == null || tb != null) {
                        logger.error("{} master fail to scan cs", ip, tb);
                        return;
                    }

                    if (rs.size() == 0) {
                        logger.info("master scanned 0 cs");
                        return;
                    }

                    for (Map.Entry<String, String> entry : rs.entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        logger.debug("master:{} scan cs:{}, heart:{} {}", ip, key, value, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(value)));
                        if (System.currentTimeMillis() - Long.parseLong(value) > 10 * checkAliveIntervalMillis) {
                            // the cs has down
                            String csIp = key.substring((wrapAliveCsKeyPrefix() + Constants.CTRL_1).length());
                            handleShutDownCS(csIp);

                            willMsgPersistManager.delete(key).whenComplete((resultDel, tbDel) -> {
                                if (!resultDel || tbDel != null) {
                                    logger.error("fail to delete shutDown cs:{} in db", key);
                                    return;
                                }
                                logger.debug("delete shutDown cs:{} in db successfully", key);
                            });
                        }
                    }
                });
            });
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    private void handleShutDownCS(String ip) {
        String startClientKey = ip + Constants.CTRL_0;
        String endClientKey = ip + Constants.CTRL_2;
        willMsgPersistManager.scan(startClientKey, endClientKey).whenComplete((willMap, throwable) -> {
            if (willMap == null || throwable != null) {
                logger.error("{} master fail to scan cs", ip, throwable);
                return;
            }

            if (willMap.size() == 0) {
                logger.info("the cs:{} has no will", ip);
                return;
            }

            for (Map.Entry<String, String> entry : willMap.entrySet()) {
                logger.info("master handle will {} {}", entry.getKey(), entry.getValue());
                String willKey = entry.getKey();
                String clientId = entry.getKey().substring((ip + Constants.CTRL_1).length());
                WillMessage willMessage = JSON.parseObject(entry.getValue(), WillMessage.class);
                sendWillMessage(willKey, clientId, willMessage);
            }
        });
    }

    public void closeConnect(Channel channel, String clientId, String reason) {
        String ip = IpUtil.getLocalAddressCompatible();
        String willKey = ip + Constants.CTRL_1 + clientId;
        CompletableFuture<byte[]> willMessageFuture = willMsgPersistManager.get(willKey);
        willMessageFuture.whenComplete((willMessageByte, throwable) -> {
            String content = new String(willMessageByte);
            if (Constants.NOT_FOUND.equals(content)) {
                return;
            }

            if (!"disconnect".equals(reason)) {
                WillMessage willMessage = JSON.parseObject(content, WillMessage.class);
                sendWillMessage(willKey, clientId, willMessage);
            }
        });
    }

    private void sendWillMessage(String willKey, String clientId, WillMessage willMessage) {
        int mqttId = mqttMsgId.nextId(clientId);
        MqttPublishMessage mqttMessage = MessageUtil.toMqttMessage(willMessage.getWillTopic(), willMessage.getBody(),
                willMessage.getQos(), mqttId, willMessage.isRetain());
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                CompletableFuture<StoreResult> r = willMsgSender.sendWillMsg(clientId, mqttMessage);
                r.whenComplete((hookResult, tb) -> {
                    try {
                        if (tb != null) {
                            logger.error("sendWillMsg failed {},{}", clientId, willKey, tb);
                        } else {
                            willMsgPersistManager.delete(willKey).whenComplete((resultDel, tbDel) -> {
                                if (!resultDel || tbDel != null) {
                                    logger.error("fail to delete will message key:{}", willKey);
                                    return;
                                }
                                logger.info("delete will message key {} successfully", willKey);
                            });
                        }
                    } catch (Throwable t) {
                        logger.error("", t);
                    }
                });
            }
        };
        executor.submit(runnable);
    }

    public void addWillMessage(Channel channel, WillMessage willMessage) {
        String clientId = ChannelInfo.getClientId(channel);
        String ip = IpUtil.getLocalAddressCompatible();
        if (willMessage == null) {
            return;
        }
        String message = JSON.toJSONString(willMessage);
        String willKey = ip + Constants.CTRL_1 + clientId;

        // key: ip + clientId; value: WillMessage
        willMsgPersistManager.put(willKey, message).whenComplete((result, throwable) -> {
            if (!result || throwable != null) {
                logger.error("fail to put will message key {} value {}", willKey, willMessage);
                return;
            }
            logger.debug("put will message key {} value {} successfully", willKey, message);
        });
    }

    public boolean isEnableLoop() {
        return enableLoop;
    }

    public void setEnableLoop(boolean enableLoop) {
        this.enableLoop = enableLoop;
    }
}
