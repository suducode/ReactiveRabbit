package com.rsa.reactive.rabbit;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Consumer configuration wiring up listener queues.
 *
 * @author Sudharshan Krishnamurthy
 */

@Configuration
@EnableAutoConfiguration
public class ConsumerConfiguration {

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Bean
    public Exchange messageExchange() {
        return new TopicExchange(Main.EXCHANGE_NAME, false, true);
    }

    @Bean
    public Queue queue() {
        return new Queue(Main.QUEUE_NAME, false, false, true);
    }

    @Bean
    public Binding amqpBinding() {
        return new Binding(Main.QUEUE_NAME, Binding.DestinationType.QUEUE, Main.EXCHANGE_NAME, "all", null);
    }

    @Bean
    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(queue());
        container.setMessageListener(receiver());
        return container;
    }

    @Bean
    ProducerProxy receiver() {
        return new ProducerProxy(rabbitTemplate);
    }

}
