package com.suducode.reactive.rabbit.consumer

import java.util.concurrent.TimeUnit

/**
 * Specification for source behavior tests
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class SourceBehaviorSpecification extends SourceBaseBehaviorSpecification {

    // Signal a demand so we have data source for tests
    def setupSpec() {
        producerProxy.signalDemand()
    }

    def "check has next is true"() {

        when: "we check for availability"
        boolean status = amqpConsumer.hasNext()

        then: "status should be available"
        status == true
    }

    def "check if you are able to pull records"() {

        when: "we poll from the source"
        def record = amqpConsumer.poll(5,TimeUnit.SECONDS)

        then: "record should not be empty"
        record != null

        cleanup:
        amqpConsumer.close()
    }

    def "check if status is updated when we pull records"() {

        when: "we poll from the source"
        5.times {
            amqpConsumer.poll(5,TimeUnit.SECONDS)
        }

        then: "consumed counter should go up"
        consumedCounter.count > 0

        cleanup:
        amqpConsumer.close()
    }
}
