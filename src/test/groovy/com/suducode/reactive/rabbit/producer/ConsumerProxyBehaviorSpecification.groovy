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
    }

    def "can process control ,control the demand ?"() {

        when : "we have a fake demand"
        createFakeDemand()

        then: "should be allowed to process"
        sourceProxy.canProcess() == true

        when: "we control the demand"
        sourceProxy.processControl(false)

        then: "should not be allowed to process"
        sourceProxy.canProcess() ==  false
    }

    def "check serialization batch completes if the time is elapsed"() {

        when : "we start the clock"
        def record = new HashMap<>()
        record.put("test", "test")
        Message message = sourceProxy.getBatchedMessage(record)

        then : "Batch not complete yet"
        message == null

        when: "wait the default configured time of 5 seconds"
        Thread.sleep(5000)
        message = sourceProxy.getBatchedMessage(record)

        then: "Batch complete"
        message != null

        cleanup:
        sourceProxy.resetSerializationBatch()
        sourceProxy.resetProcessedCount()
    }


    def "check serialization batch completes with the configured count"() {

        when : "we start the batch"
        def record = new HashMap<>()
        record.put("test", "test")
        Message message = sourceProxy.getBatchedMessage(record)

        then : "Batch not complete yet"
        message == null

        when: "sending the rest to complete the batch "
        4.times {
            message = sourceProxy.getBatchedMessage(record)
        }

        then: "Batch complete"
        message != null

        cleanup:
        sourceProxy.resetSerializationBatch()
        sourceProxy.resetProcessedCount()
    }

    def "check sending message to broker"() {

        when : "sending message to broker in a serialized batch"
        sourceProxy.setSerializationCount(5)
        sourceProxy.sendMessageToBroker(new Message(new byte[sourceProxy.getSerializationCount()],new MessageProperties()))

        then : "counter should go up serialization batch size"
        sourceProxy.getBatchProcessedCount() == 5

        cleanup:
        sourceProxy.resetSerializationBatch()
        sourceProxy.resetProcessedCount()
    }

    def "check source proxy processes records correctly"() {

        when : "records offered to be processed one less than publish batch size count"
        def record = new HashMap<>()
        record.put("test", "test")
        9.times {
           sourceProxy.process(record)
        }

        then : "counter should go up by serialization batch size"
        sourceProxy.getBatchProcessedCount() == 5

        when: "sending one more to complete the batch"
        sourceProxy.process(record)

        then : "counter should be reset"
        sourceProxy.getBatchProcessedCount() == 0

        cleanup:
        sourceProxy.resetSerializationBatch()
        sourceProxy.resetProcessedCount()
    }

}
