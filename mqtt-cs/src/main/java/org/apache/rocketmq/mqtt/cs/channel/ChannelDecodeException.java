package org.apache.rocketmq.mqtt.cs.channel;


public class ChannelDecodeException extends RuntimeException {
    public ChannelDecodeException() {}

    public ChannelDecodeException(String message) {
        super(message);
    }

    public ChannelDecodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChannelDecodeException(Throwable cause) {
        super(cause);
    }

    public ChannelDecodeException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
