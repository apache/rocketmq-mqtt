package org.apache.rocketmq.mqtt.ds.upstream.coap.processor;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.mqtt.common.facade.LmqQueueStore;
import org.apache.rocketmq.mqtt.common.facade.RetainedPersistManager;
import org.apache.rocketmq.mqtt.common.hook.HookResult;
import org.apache.rocketmq.mqtt.common.model.CoapRequestMessage;
import org.apache.rocketmq.mqtt.common.model.Message;
import org.apache.rocketmq.mqtt.common.model.MqttTopic;
import org.apache.rocketmq.mqtt.common.model.StoreResult;
import org.apache.rocketmq.mqtt.common.util.MessageUtil;
import org.apache.rocketmq.mqtt.common.util.TopicUtils;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.meta.WildcardManager;
import org.apache.rocketmq.mqtt.ds.upstream.coap.CoapUpstreamProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.mqtt.processor.PublishProcessor;
import org.apache.rocketmq.mqtt.exporter.collector.MqttMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Component
public class CoapPublishProcessor implements CoapUpstreamProcessor {
    private static Logger logger = LoggerFactory.getLogger(PublishProcessor.class);

    @Resource
    private LmqQueueStore lmqQueueStore;

    @Resource
    private WildcardManager wildcardManager;

    @Resource
    private FirstTopicManager firstTopicManager;

    @Resource
    RetainedPersistManager retainedPersistManager;

    @Override
    public CompletableFuture<HookResult> process(CoapRequestMessage msg) {
        CompletableFuture<StoreResult> r = put(msg);
        return r.thenCompose(storeResult -> HookResult.newHookResult(HookResult.SUCCESS, null,
                JSON.toJSONBytes(storeResult)));
    }

    public CompletableFuture<StoreResult> put(CoapRequestMessage coapMessage) {
        // todo: process topic alias

        boolean isEmpty = false;

        // deal empty payload
        if (coapMessage.getPayload() == null || coapMessage.getPayload().length == 0) {
            coapMessage.setPayload(MessageUtil.EMPTYSTRING.getBytes(StandardCharsets.UTF_8));
            isEmpty = true;
        }

        String originTopic = coapMessage.getTopic();
        String pubTopic = TopicUtils.normalizeTopic(originTopic);
        MqttTopic mqttTopic = TopicUtils.decode(pubTopic);
        firstTopicManager.checkFirstTopicIfCreated(mqttTopic.getFirstTopic()); // Check the firstTopic is existed
        Set<String> queueNames = wildcardManager.matchQueueSetByMsgTopic(pubTopic, null); // Find queues by topic

        String msgId = MessageClientIDSetter.createUniqID();
        long bornTime = System.currentTimeMillis();

        if (coapMessage.isReatin()) {
            CoapRequestMessage retainedCoapMessage = coapMessage.copy();
            //Change the retained flag of message that will send MQ is 0
            retainedCoapMessage.setReatin(false);
            // store retained message
            Message metaMessage = MessageUtil.toMessage(retainedCoapMessage);
            metaMessage.setMsgId(msgId);
            metaMessage.setBornTimestamp(bornTime);
            metaMessage.setEmpty(isEmpty);
            CompletableFuture<Boolean> storeRetainedFuture = retainedPersistManager.storeRetainedMessage(TopicUtils.normalizeTopic(metaMessage.getOriginTopic()), metaMessage);
            storeRetainedFuture.whenComplete((res, throwable) -> {
                if (throwable != null) {
                    logger.error("Store topic:{} retained message error.{}", metaMessage.getOriginTopic(), throwable);
                }
            });
        }

        Message message = MessageUtil.toMessage(coapMessage);
        message.setMsgId(msgId);
        message.setBornTimestamp(bornTime);
        message.setEmpty(isEmpty);

        collectWriteBytesAndTps(message.getFirstTopic(), message.getPayload().length);

        return lmqQueueStore.putMessage(queueNames, message);

    }

    private void collectWriteBytesAndTps(String topic, int length) {
        try {
            MqttMetricsCollector.collectReadWriteMatchActionBytes(length, topic, "put");
            MqttMetricsCollector.collectPutRequestTps(1, topic);
        } catch (Throwable e) {
            logger.error("Collect prometheus error", e);
        }
    }
}
