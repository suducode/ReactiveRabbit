package com.suducode.reactive.rabbit.consumer


import com.suducode.reactive.rabbit.common.CommonSpecification
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties
import com.suducode.reactive.rabbit.producer.ReactiveProducer
import spock.lang.Shared

import java.util.concurrent.TimeUnit

/**
 * AMQP consumer testing.
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class ConsumerSpecification extends CommonSpecification {

    @Shared
    ReactiveConsumer amqpConsumer;

    @Shared
    ReactiveProducer amqpProducer;


    def "check if we are able to configure processor with defaults correctly"() {

        when: "Configure just properties with unique identifier"
        amqpConsumer = new ReactiveConsumer<>()

        then: "no exception thrown"
        noExceptionThrown()

        cleanup:
        amqpConsumer.close()
        cleanupInfrastructure()
    }

    def "check if we are able to setup basic broker infrastructure when we start a source"() {

        when: "just properties with unique identifier and a demand"
        amqpConsumer = new ReactiveConsumer<>()


        then: "check if necessary queues and exchanges are created"
        checkQueue("reactive_exchange_message_queue")
        checkQueue("reactive_exchange_reply_queue")

        cleanup:
        amqpConsumer.close()
        cleanupInfrastructure()

    }


    def "check if we are able to receive records from the broker queue we are listening"() {

        when: "just properties with unique identifier and a demand"
        amqpConsumer = new ReactiveConsumer()
        createSource()

        then: "we should be able to get records from the broker"
        amqpConsumer.poll(5, TimeUnit.SECONDS) != null

        cleanup:
        amqpConsumer.close()
        amqpProducer.close()
        cleanupInfrastructure()
    }

    def "check if no exception is thrown when a demand is issued"() {

        when: "we setup a source and a receiver that can hijack our demand"

        //first make sure the cleanup has happened before proceeding.
        ReactiveAmqpProperties properties = new ReactiveAmqpProperties()
        properties.setPublishBatchSize(1);
        amqpConsumer = new ReactiveConsumer(properties)
        setupReceiver("reactive_exchange_reply_queue")
        amqpConsumer.advanceAggregation()

        then: "no exception is thrown"
        noExceptionThrown()

        cleanup:
        amqpConsumer.close()
        cleanupInfrastructure()
    }

    /**
     * When creating a source we create a demand as well so the processor should be able
     * to publish events to broker and have it ready for the source to consume.
     */
    def void createSource() {
        def properties = new ReactiveAmqpProperties()
        properties.setPublishBatchSize(10)
        properties.setAmqpBatchSize(2)
        properties.setSerializationBatchSize(5)
        properties.setSerializationBatchTimeout(5000)
        amqpProducer = new ReactiveProducer<>(properties)

        10.times {
            def record = new HashMap<>()
            record.put("time", System.currentTimeSeconds())
            record.put("sessionid", 1l)
            record.put("test", new byte[500])
            while (amqpProducer.push(record));
        }
    }
}