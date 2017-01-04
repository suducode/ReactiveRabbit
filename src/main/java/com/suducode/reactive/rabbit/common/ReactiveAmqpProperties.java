package com.suducode.reactive.rabbit.common;

import java.util.UUID;

/**
 * Reactive AMQP Configuration properties.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ReactiveAmqpProperties {

    /**
     * Unique Identifier
     */
    private String id = String.valueOf(UUID.randomUUID());

    public static final String REPLY_QUEUE_SUFFIX = "_reply_queue";

    public static final String REPLY_ROUTING_KEY = "reply";

    protected static final String MESSAGE_QUEUE_SUFFIX = "_message_queue";

    public static final String MESSAGE_ROUTING_KEY = "message";

    /**
     * Message exchange to which the messages are published by the amqp processor.
     */

    private String exchange = "reactive_exchange";

    /**
     * Agreed upon batch size between amqp producer and consumer for records transfer.
     */
    private int publishBatchSize = 100000;

    /**
     * AMQP broker hostname
     */
    private String amqpHostName = "localhost";


    /**
     * AMQP broker port number
     */
    private int amqpPortNumber = 5672;

    /**
     * AMQP virtual host definition
     */
    private String amqpVirtualHost = "/";

    /**
     * AMQP user name
     */
    private String userName = "guest";

    /**
     * AMQP password
     */
    private String Password = "guest";

    /**
     * AMQP batch size used by the batched template
     */
    private int amqpBatchSize = 100;

    /**
     * The maximum amount of time, in milliseconds, to wait before sending a batch.  At the timeout, the
     * batch will be sent regardless of the number of messages.
     */

    private int amqpBatchTimeout = 60000;

    /**
     * Records serialization batch size used before sending to AMQP broker.
     */
    private int serializationBatchSize = 100;

    /**
     * The maximum amount of time, in milliseconds, to wait before creating a batch for serialization.
     * At the timeout, the batch will be serialized regardless of the number of messages.
     */
    private int serializationBatchTimeout = 30000;

    /**
     * Average event size in bytes. This is used by batched template to calculate the max buffer size.
     * Default is on a higher side to avoid unnecessary mini batching that might take place.
     */
    private int averageEventSizeInBytes = 1500;

    /**
     * Prefetch defines behavior to send messages in advance of more message that client has unacked.
     * It is used to save time on network operations to wait for a new messages.
     * refer for suggestions : https://www.rabbitmq.com/blog/2012/05/11/some-queuing-theory-throughput-latency-and-bandwidth/
     */
    private int ConsumerPrefetchCount = 30;

    /**
     * It defines how many threads are used by the consumer to drain the events from the Queue.
     * It is performance optimization in most cases to have multiple threads but it does not guaranteed
     * to maintain ordering and hence the default will be 1.
     */
    private int ConsumerConcurrentCount = 1;


    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public int getPublishBatchSize() {
        return publishBatchSize;
    }

    public void setPublishBatchSize(int publishBatchSize) {
        this.publishBatchSize = publishBatchSize;
    }

    public String getAmqpHostName() {
        return amqpHostName;
    }

    public void setAmqpHostName(String amqpHostName) {
        this.amqpHostName = amqpHostName;
    }

    public int getAmqpPortNumber() {
        return amqpPortNumber;
    }

    public void setAmqpPortNumber(int amqpPortNumber) {
        this.amqpPortNumber = amqpPortNumber;
    }

    public String getAmqpVirtualHost() {
        return amqpVirtualHost;
    }

    public void setAmqpVirtualHost(String amqpVirtualHost) {
        this.amqpVirtualHost = amqpVirtualHost;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return Password;
    }

    public void setPassword(String password) {
        Password = password;
    }

    public int getAmqpBatchSize() {
        return amqpBatchSize;
    }

    public void setAmqpBatchSize(int amqpBatchSize) {
        this.amqpBatchSize = amqpBatchSize;
    }

    public int getAmqpBatchTimeout() {
        return amqpBatchTimeout;
    }

    public void setAmqpBatchTimeout(int amqpBatchTimeout) {
        this.amqpBatchTimeout = amqpBatchTimeout;
    }

    public int getSerializationBatchSize() {
        return serializationBatchSize;
    }

    public void setSerializationBatchSize(int serializationBatchSize) {
        this.serializationBatchSize = serializationBatchSize;
    }

    public int getSerializationBatchTimeout() {
        return serializationBatchTimeout;
    }

    public void setSerializationBatchTimeout(int serializationBatchTimeout) {
        this.serializationBatchTimeout = serializationBatchTimeout;
    }

    public int getAverageEventSizeInBytes() {
        return averageEventSizeInBytes;
    }

    public void setAverageEventSizeInBytes(int averageEventSizeInBytes) {
        this.averageEventSizeInBytes = averageEventSizeInBytes;
    }

    public int getConsumerPrefetchCount() {
        return ConsumerPrefetchCount;
    }

    public void setConsumerPrefetchCount(int consumerPrefetchCount) {
        ConsumerPrefetchCount = consumerPrefetchCount;
    }

    public int getConsumerConcurrentCount() {
        return ConsumerConcurrentCount;
    }

    public void setConsumerConcurrentCount(int consumerConcurrentCount) {
        ConsumerConcurrentCount = consumerConcurrentCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
