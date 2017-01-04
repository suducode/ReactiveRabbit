package com.suducode.reactive.rabbit.common;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

/**
 * Amqp Configuration that is common to both source and processor.
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
public class ReactiveAmqpConfiguration {

    protected ReactiveAmqpProperties configurationProperties;

    protected ConnectionFactory connectionFactory;

    private Queue replyQueue;

    private Queue messageQueue;

    public ReactiveAmqpConfiguration(ReactiveAmqpProperties configurationProperties) {

        this.configurationProperties = configurationProperties;
        this.connectionFactory = createConnectionFactory();

        // Now lets go setup the rabbit infrastructure for communication
        setupInfrastructure();
    }

    private void setupInfrastructure() {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

        // setup exchange
        String exchangeName = configurationProperties.getExchange();
        Exchange reactiveExchange = new TopicExchange(exchangeName, false, true);
        rabbitAdmin.declareExchange(reactiveExchange);

        // setup reply queue
        String replyQueueName = exchangeName + ReactiveAmqpProperties.REPLY_QUEUE_SUFFIX;
        replyQueue = new Queue(replyQueueName, false, false, true);
        rabbitAdmin.declareQueue(replyQueue);

        // setup reply queue binding
        Binding replyQueueBinding = new Binding(replyQueueName,
                Binding.DestinationType.QUEUE, exchangeName,
                ReactiveAmqpProperties.REPLY_ROUTING_KEY, null);
        rabbitAdmin.declareBinding(replyQueueBinding);

        // setup message queue
        String messageQueueName = exchangeName + ReactiveAmqpProperties.MESSAGE_QUEUE_SUFFIX;
        messageQueue = new Queue(messageQueueName, false, false, true);
        rabbitAdmin.declareQueue(messageQueue);

        // setup message queue binding
        Binding messageQueueBinding = new Binding(messageQueueName,
                Binding.DestinationType.QUEUE, exchangeName,
                ReactiveAmqpProperties.MESSAGE_ROUTING_KEY, null);
        rabbitAdmin.declareBinding(messageQueueBinding);
    }


    /**
     * Create a connection factory from connection details provided
     *
     * @return connectionFactory instance
     */
    private ConnectionFactory createConnectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory(configurationProperties.getAmqpHostName(),
                        configurationProperties.getAmqpPortNumber());
        connectionFactory.setVirtualHost(configurationProperties.getAmqpVirtualHost());
        connectionFactory.setUsername(configurationProperties.getUserName());
        connectionFactory.setPassword(configurationProperties.getPassword());
        return connectionFactory;
    }

    /**
     * This returns a reply queue from which amqp processor gets the next demand.
     *
     * @return Reply Queue
     */
    public Queue replyQueue() {
        return replyQueue;
    }

    /**
     * This returns a message queue from which amqp source receives published messages
     *
     * @return a message queue
     */
    public Queue messageQueue() {
        return messageQueue;
    }

}

