package com.suducode.reactive.rabbit.consumer

import com.suducode.reactive.rabbit.common.CommonSpecification
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties


/**
 * Testing the resilience of the system
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
public class ResilienceSpecification extends CommonSpecification {

    def "check the demand supply resilience"() {

        when:
        "we setup a consumer and a receiver that can hijack our demand"
        ReactiveAmqpProperties properties = new ReactiveAmqpProperties()
        properties.setAmqpBatchTimeout(2000)
        ReactiveConsumer consumer = new ReactiveConsumer(properties)
        setupReceiver("reactive_exchange_reply_queue")
        Thread.sleep(10000) // Demand Check Scheduler frequency

        then:
        "demand gets reissued after stipulated time"
        received == 2 // The original demand and the re issued demand

        cleanup:
        consumer.close()
        cleanupInfrastructure()
    }
}