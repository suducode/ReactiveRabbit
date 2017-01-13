package com.suducode.reactive.rabbit.producer
/**
 * Tests the behavior of the producer
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class ProducerBehaviorSpecification extends ProducerBaseBehaviorSpecification {

    def "check accept when there is demand"() {

        given : "Default setup and creating a fake demand"
        createFakeDemand()

        when: "Send 100 records"
        100.times {
            def record = new HashMap<>()
            record.put("test", "test")
            producer.push(record)
        }

        then: "Count should match the number of records sent"
        counter.count == 100

        cleanup:
        producer.close()
    }

    def "check accept when there is no demand"() {

        given: "Default setup and creating a fake no demand situation"
        createFakeNoDemand()

        when: "Send a record"
        def record = new HashMap<>()
        record.put("test", "test")
        producer.push(record)

        then: "Counter should not go up since there is no demand"
        counter.count == 0

        when: "Create a demand again ans send the record back"
        createFakeDemand()
        producer.push(record)

        then: "Counter goes up"
        counter.count == 1

        cleanup:
        producer.close()
    }
}
