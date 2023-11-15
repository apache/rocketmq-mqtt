package org.apache.rocketmq.mqtt.cs.protocol;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttVersion;

public interface ChannelPipelineLazyInit {
    void init(ChannelPipeline channelPipeline, MqttVersion mqttVersion);
}
