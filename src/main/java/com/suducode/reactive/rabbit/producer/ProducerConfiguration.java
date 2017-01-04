package com.suducode.reactive.rabbit.producer;

import com.suducode.reactive.rabbit.common.BatchSerializedTemplate;
import com.suducode.reactive.rabbit.common.ReactiveAmqpConfiguration;
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Producer configuration.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ProducerConfiguration extends ReactiveAmqpConfiguration {

    private ConsumerProxy consumerProxy;

    private SimpleMessageListenerContainer container;

    ProducerConfiguration(ReactiveAmqpProperties configurationProperties) {
        super(configurationProperties);

        // Now lets go setup the rabbit infrastructure for communication
        setupProcessorInfrastructure();
    }

    private void setupProcessorInfrastructure() {

        BatchSerializedTemplate batch = batchingRabbitTemplate();

        // create a proxy for the source through which broker communication happens.
        consumerProxy = new ConsumerProxy(batch);

        // Tells which method would receive the demand.
        MessageListenerAdapter listenerAdapter = new MessageListenerAdapter(consumerProxy, "receive");

        container = new SimpleMessageListenerContainer(); // setup the container
        container.setMessageListener(listenerAdapter);
        container.setConnectionFactory(connectionFactory);
        container.setQueues(replyQueue());
        container.start();
    }

    ConsumerProxy getConsumerProxy() {
        return consumerProxy;
    }

    /**
     * Creates a scheduler to be used by the batching template
     *
     * @return ThreadPoolTaskScheduler instance
     */
    private ThreadPoolTaskScheduler scheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1); // we will have one to maintain the event ordering.
        scheduler.setThreadNamePrefix("BatchTemplatePool-");
        scheduler.afterPropertiesSet();
        return scheduler;
    }

    /**
     * Setting up batched template
     *
     * @return BatchingRabbitTemplate
     */
    private BatchSerializedTemplate batchingRabbitTemplate() {

        int amqpBatchSize = configurationProperties.getAmqpBatchSize();
        int serializationBatchSize = configurationProperties.getSerializationBatchSize();
        int averageEventByteSize = configurationProperties.getAverageEventSizeInBytes();
        int batchBufferLimit = amqpBatchSize * serializationBatchSize * averageEventByteSize;
        int amqpBatchTimeout = configurationProperties.getAmqpBatchTimeout();

        SimpleBatchingStrategy strategy = new SimpleBatchingStrategy(amqpBatchSize,
                batchBufferLimit, amqpBatchTimeout);

        BatchSerializedTemplate batch = new BatchSerializedTemplate(strategy, scheduler(), configurationProperties);
        batch.setConnectionFactory(connectionFactory);
        batch.afterPropertiesSet();
        return batch;
    }

    void cleanup() {
        container.stop();
    }

}
