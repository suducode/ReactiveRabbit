package com.suducode.reactive.rabbit.producer

import com.suducode.reactive.rabbit.common.CommonSpecification
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties
import spock.lang.Shared

/**
 * AMQP Producer testing. RabbitMQ broker should be up and running for these tests to work.
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */

class ProducerSpecification extends CommonSpecification {

    @Shared
    ReactiveProducer amqpProducer;

    def "check if we are able to configure producer with defaults correctly"() {

        given: "default producer setup and a demand"
        amqpProducer = new ReactiveProducer()
        createDemand(amqpProducer)

        when: "we push events to the producer"
        amqpProducer.push(new HashMap<String, Object>())

        then: "no exception should be thrown and the counter should go up"
        noExceptionThrown()

        when: "we push events to the producer"
        amqpProducer.push(new HashMap<String, Object>())

        then: "no exception should be thrown and the counter should go up"
        amqpProducer.getProcessedCount() == 2

        cleanup:
        amqpProducer.close()
        cleanupInfrastructure()
    }

    def "check if we are able setup the broker infrastructure"() {

        when: "we setup a producer"
        amqpProducer = new ReactiveProducer()

        then: "check if the broker queue is created "
        checkQueue("reactive_exchange_message_queue")

        cleanup:
        amqpProducer.close()
        cleanupInfrastructure()
    }


    def "check if throw exception when configuration is not right"() {

        when: "wrong configuration with AmqpBatchSize times SerializationBatchSize not equal to PublishBatchSize"
        def properties = new ReactiveAmqpProperties();
        properties.setPublishBatchSize(10)
        properties.setAmqpBatchSize(6)
        properties.setSerializationBatchSize(9)
        amqpProducer = new ReactiveProducer(properties)
        createDemand(amqpProducer)

        then: "throw an exception"
        thrown(RuntimeException.class)

        cleanup:
        cleanupInfrastructure()
    }

    def "check if we are able to push records successfully to the broker"() {

        given: "basic config properties and a demand"
        def properties = new ReactiveAmqpProperties()
        properties.setPublishBatchSize(10)
        properties.setSerializationBatchSize(5)
        properties.setAmqpBatchSize(2)
        properties.setSerializationBatchTimeout(5000)
        amqpProducer = new ReactiveProducer<>(properties)
        createDemand(amqpProducer)

        when: "we push events to the processor"
        50.times {
            def record = new HashMap<>()
            record.put("test", new byte[1000])
            amqpProducer.push(record)
        }

        then: "no exception should be thrown"
        getQueueSize("reactive_exchange_message_queue") > 0

        cleanup:
        amqpProducer.close()
        cleanupInfrastructure()
    }

    def "check if we throw exception when configuration is such that we can give predictable performance behaviour"() {

        when : "unacceptable config properties and a demand"
        def properties = new ReactiveAmqpProperties()
        properties.setPublishBatchSize(10)
        properties.setSerializationBatchSize(7)
        properties.setAmqpBatchSize(2)
        properties.setSerializationBatchTimeout(5000)
        amqpProducer = new ReactiveProducer<>(properties)
        createDemand(amqpProducer)

        then: "exception should be thrown"
        thrown(RuntimeException)

        cleanup:
        cleanupInfrastructure()
    }

    /**
     * We are creating a fake demand instead of bringing up a consumer unnecessarily which is
     * what would create a the demand in the real use case.
     * @param processor
     */

    def void createDemand(ReactiveProducer producer) {
        producer.request(10)
    }

    /**
     * Creates a no demand situation
     * @param processor
     */
    def void createNoDemand(ReactiveProducer producer) {
        producer.request(0)
    }
}