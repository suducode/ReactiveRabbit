package com.suducode.reactive.rabbit.producer

import com.codahale.metrics.Counter

import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import spock.lang.Shared
import spock.lang.Specification

import static org.mockito.Matchers.any
import static org.mockito.Matchers.anyLong
import static org.mockito.Mockito.doAnswer
import static org.mockito.Mockito.mock

/**
 * Base class for producer behavior related tests
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class ProducerBaseBehaviorSpecification extends Specification {

    @Shared
    ReactiveProducer producer;

    @Shared
    ConsumerProxy consumerProxy;

    @Shared
    Counter counter = new Counter();

    @Shared
    boolean isAvailable = false;

    def setupSpec() {
        producer = mock(ReactiveProducer.class)
        consumerProxy = mock(ConsumerProxy.class)
        mockProducerAccept()
        mockConsumerProxyReceive()
        mockConsumerProxyOnNext()
        mockProducerClose()
    }

    def mockProducerAccept() {
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(final InvocationOnMock invocation) throws Throwable {
                if (!isAvailable) {
                    return false
                }
                counter.inc(1)
                return true
            }
        }).when(producer).push(any())
    }

    def mockProducerClose() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                counter.dec(counter.count)
                isAvailable = false
                return null
            }
        }).when(producer).close()
    }

    def createFakeDemand() {
        isAvailable = true
    }

    def createFakeNoDemand() {
        isAvailable = false
    }

    def mockConsumerProxyReceive() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                long request = (Long) invocation.arguments[0];
                if (request > 0) {
                    isAvailable = true
                } else {
                    isAvailable = false
                }
                return null
            }
        }).when(consumerProxy).receive(anyLong())
    }

    def mockConsumerProxyOnNext() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                counter.inc()
                if (counter.count == 10) {
                    counter.dec(counter.count)
                }
                return null
            }
        }).when(consumerProxy).onNext(any())
    }

    def Properties defaultSetup() {
        ReactiveAmqpProperties amqpProperties = new ReactiveAmqpProperties();
        amqpProperties.setId("producer1");
        amqpProperties.setPublishBatchSize(10);
        amqpProperties.setSerializationBatchSize(5);
        amqpProperties.setAmqpBatchSize(2);
        amqpProperties.setSerializationBatchTimeout(5000);
        return properties
    }
}
