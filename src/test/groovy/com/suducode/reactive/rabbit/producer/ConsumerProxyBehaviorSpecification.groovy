package com.suducode.reactive.rabbit.producer

import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties

/**
 * Tests the behavior of Consumer Proxy which the Amqp producer heavily relies on.
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class ConsumerProxyBehaviorSpecification extends ProducerBaseBehaviorSpecification {

    def "check if receive demand sets the correct state"() {

        when : "we have a fake demand"
        createFakeDemand()

        then: "should be allowed to process"
        producer.push(new HashMap()) == true

        when : "no demand"
        createFakeNoDemand()

        then: "should not be allowed to process"
        producer.push(new HashMap()) == false

        cleanup:
        producer.close()
    }

    def "can process control ,control the demand ?"() {

        when : "we have a fake demand"
        createFakeDemand()

        then: "should be allowed to process"
        producer.push(new HashMap()) == true

        when: "we control the demand"
        consumerProxy.receive(0)

        then: "should not be allowed to process"
        producer.push(new HashMap()) ==  false

        cleanup:
        producer.close()
    }

    def "check to make sure OnNext works as expected"() {

        when : "create a record and push it to the proxy"
        def record = new HashMap<>()
        record.put("test", "test")
        consumerProxy.onNext(record)

        then : "check to make sure our counter goes up"
        counter.count == 1

        when: "wait the default configured time of 5 seconds"
        9.times {
            consumerProxy.onNext(record)
        }

        then: "Batch should be complete and counter should be reset"
        counter.count == 0

        cleanup:
        consumerProxy.cleanup()
    }

}
