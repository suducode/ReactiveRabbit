package com.suducode.reactive.rabbit.consumer;

import com.suducode.reactive.rabbit.common.ReactiveAmqpConfiguration;
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

/**
 * Consumer configuration.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ConsumerConfiguration extends ReactiveAmqpConfiguration {

    private ProducerProxy producerProxy;

    private SimpleMessageListenerContainer container;

    ConsumerConfiguration(ReactiveAmqpProperties configurationProperties) {
        super(configurationProperties);

        // Now lets go setup the rabbit infrastructure for communication
        setupSourceInfrastructure();
    }

    private void setupSourceInfrastructure() {

        // Gets the rabbit template through which the demand can be sent to the broker.
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.afterPropertiesSet();

        // create a proxy for the processor through which broker communication happens.
        producerProxy = new ProducerProxy(rabbitTemplate, configurationProperties);

        // Sets up the receiver for records that are published to the broker.
        container = new SimpleMessageListenerContainer();
        container.setMissingQueuesFatal(true);
        container.setConcurrentConsumers(configurationProperties.getConsumerConcurrentCount());
        container.setPrefetchCount(configurationProperties.getConsumerPrefetchCount());
        container.setConnectionFactory(connectionFactory);
        container.setQueues(messageQueue());
        container.setMessageListener(producerProxy);
        container.start();
    }

    protected ProducerProxy getProducerProxy() {
        return producerProxy;
    }

    void cleanup() {
        container.stop();
    }


}
