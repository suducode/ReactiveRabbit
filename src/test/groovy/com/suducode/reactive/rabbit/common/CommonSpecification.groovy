package com.suducode.reactive.rabbit.common

import com.rabbitmq.client.*
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.core.ChannelCallback
import org.springframework.amqp.rabbit.core.RabbitAdmin
import org.springframework.amqp.rabbit.core.RabbitTemplate
import spock.lang.Specification

/**
 * Common reusable framework specification
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */

class CommonSpecification extends Specification {

    def received  = 0

    def int getQueueSize(String queueName) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(getConnectionFactory())
        AMQP.Queue.DeclareOk declareOk = rabbitTemplate.execute(new ChannelCallback<AMQP.Queue.DeclareOk>() {
            @Override
            AMQP.Queue.DeclareOk doInRabbit(Channel channel) throws Exception {
                return channel.queueDeclarePassive(queueName)
            }
        })
        return declareOk.getMessageCount()
    }

    def boolean checkQueue(String queueName) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(getConnectionFactory())
        AMQP.Queue.DeclareOk declareOk = rabbitTemplate.execute(new ChannelCallback<AMQP.Queue.DeclareOk>() {
            @Override
            AMQP.Queue.DeclareOk doInRabbit(Channel channel) throws Exception {
                return channel.queueDeclarePassive(queueName)
            }
        })
        return (declareOk.getQueue() != null)
    }


    def void setupReceiver(String queueName) {

        com.rabbitmq.client.ConnectionFactory factory = new com.rabbitmq.client.ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        if(!checkQueue(queueName)) {
            channel.queueDeclare(queueName, false, false, true, null);
        }
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                received++
                System.out.println(" [x] Received '" + Boolean.parseBoolean(message) + "'");
            }
        };

        channel.basicConsume(queueName, true, consumer);
    }

    def boolean deleteQueue(String queueName) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(getConnectionFactory())
        rabbitAdmin.deleteQueue(queueName)
    }

    def ConnectionFactory getConnectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory("localhost", 5672)
        connectionFactory.setVirtualHost("/")
        connectionFactory.setUsername("guest")
        connectionFactory.setPassword("guest")
        return connectionFactory
    }

    def void cleanupInfrastructure() {
        deleteQueue("reactive_exchange_message_queue")
        deleteQueue("reactive_exchange_reply_queue")
    }
}
