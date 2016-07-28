package com.rsa.reactive.rabbit;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Deals with producer configuration of setting up bath te
 *
 * @author Sudharshan Krishnamurthy
 */
@Configuration
@EnableAutoConfiguration
public class ProducerConfiguration {

    @Bean
    ProducerProperties properties() {
        return new ProducerProperties();
    }

    @Bean
    public ThreadPoolTaskScheduler scheduler(ProducerProperties properties) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(properties.getBatchThreadCount());
        scheduler.setDaemon(true);
        scheduler.setThreadNamePrefix("BatchTemplatePool");
        return scheduler;
    }

    @Bean
    public BatchingRabbitTemplate batchingRabbitTemplate(ProducerProperties properties,
                                                         ConnectionFactory connectionFactory, ThreadPoolTaskScheduler scheduler) {

        SimpleBatchingStrategy strategy = new SimpleBatchingStrategy(properties.getBatchSize(),
                properties.getBatchBufferLimit(), properties.getBatchTimeout());

        BatchingRabbitTemplate batch = new BatchingRabbitTemplate(strategy, scheduler);
        batch.setConnectionFactory(connectionFactory);
        batch.afterPropertiesSet();
        return batch;
    }


    @Bean
    public SimpleMessageListenerContainer replyListenerContainer(BatchingRabbitTemplate batchingRabbitTemplate, ConnectionFactory connectionFactory) {

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueues(replyQueue());
        container.setMessageListener(listenerAdapter(receiver()));
        return container;
    }


    @Bean
    ConsumerProxy receiver() {
        return new ConsumerProxy();
    }

    @Bean
    MessageListenerAdapter listenerAdapter(ConsumerProxy receiver) {
        return new MessageListenerAdapter(receiver, "receive");
    }

    @Bean
    public Queue replyQueue() {
        return new Queue(Main.REPLY_QUEUE_NAME, false, false, true);
    }

    @Bean
    public Exchange replyExchange() {
        return new TopicExchange(Main.REPLY_EXCHANGE_NAME, false, true);
    }

    @Bean
    public Binding replyAmqpBinding() {
        return new Binding(Main.REPLY_QUEUE_NAME, Binding.DestinationType.QUEUE, Main.REPLY_EXCHANGE_NAME, "all", null);
    }

}
