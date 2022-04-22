package org.apache.rocketmq.mqtt.ds.test;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.mqtt.common.facade.SubscriptionPersistManager;
import org.apache.rocketmq.mqtt.common.model.MqttMessageUpContext;
import org.apache.rocketmq.mqtt.ds.meta.FirstTopicManager;
import org.apache.rocketmq.mqtt.ds.upstream.processor.SubscribeProcessor;
import org.apache.rocketmq.mqtt.ds.upstream.processor.UnSubscribeProcessor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestUnSubscribeProcessor {

    @Mock
    private FirstTopicManager firstTopicManager;

    @Mock
    private SubscriptionPersistManager subscriptionPersistManager;

    @Test
    public void test() throws IllegalAccessException {
        UnSubscribeProcessor unSubscribeProcessor = new UnSubscribeProcessor();
        FieldUtils.writeDeclaredField(unSubscribeProcessor, "firstTopicManager", firstTopicManager, true);
        FieldUtils.writeDeclaredField(unSubscribeProcessor, "subscriptionPersistManager", subscriptionPersistManager, true);

        MqttMessageUpContext context = new MqttMessageUpContext();
        context.setClientId("test");

        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 1);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(1);
        MqttUnsubscribePayload payload = new MqttUnsubscribePayload(Arrays.asList("test"));
        MqttUnsubscribeMessage mqttUnsubscribeMessage = new MqttUnsubscribeMessage(mqttFixedHeader, variableHeader, payload);

        unSubscribeProcessor.process(context, mqttUnsubscribeMessage);
        verify(firstTopicManager).checkFirstTopicIfCreated(any());
        verify(subscriptionPersistManager).removeSubscriptions(any(), anySet());
    }

}
