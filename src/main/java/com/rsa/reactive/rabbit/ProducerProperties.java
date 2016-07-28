package com.rsa.reactive.rabbit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Producer properties
 */
@Component
@ConfigurationProperties(prefix = "amqp.producer")
public class ProducerProperties {

    /**
     * The number of threads to use send the batched messages
     */
    private int batchThreadCount = 8;

    /**
     * The number of messages to batch together before sending
     */
    private int batchSize = 512;

    /**
     * The maximum size, in bytes, of the batch buffer. Hitting this limit will cause the batch
     * to be sent regardless of the number of messages.
     */
    private int batchBufferLimit = 1024 * 1024 * 2; // 2MB

    /**
     * The maximum amount of time, in milliseconds, to wait before sending a batch.  At the timeout, the
     * batch will be sent regardless of the number of messages.
     */
    private long batchTimeout = 60_000; // 1 minute

    /**
     * The number of messages to send
     */
    private int messageCount = 1_000_000;

    /**
     * The size of the message payload
     */
    private int messageSize = 384;

    /**
     * The number of synchronous messages to send during warmup
     */
    private int warmupMessageCount = 10;

    public int getBatchThreadCount() {
        return batchThreadCount;
    }

    public void setBatchThreadCount(int batchThreadCount) {
        this.batchThreadCount = batchThreadCount;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchBufferLimit() {
        return batchBufferLimit;
    }

    public void setBatchBufferLimit(int batchBufferLimit) {
        this.batchBufferLimit = batchBufferLimit;
    }

    public long getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(long batchTimeout) {
        this.batchTimeout = batchTimeout;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public int getWarmupMessageCount() {
        return warmupMessageCount;
    }

    public void setWarmupMessageCount(int warmupMessageCount) {
        this.warmupMessageCount = warmupMessageCount;
    }
}
