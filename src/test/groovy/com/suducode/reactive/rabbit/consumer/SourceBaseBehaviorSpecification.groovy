package com.suducode.reactive.rabbit.consumer

import com.codahale.metrics.Counter
import com.fasterxml.jackson.databind.ObjectMapper
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.msgpack.jackson.dataformat.MessagePackFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.mockito.Mockito.*

/**
 * Base specification for source behavior tests
 *
 * @author Sudharshan Krishnamurthy
 * @version 11.0
 */
class SourceBaseBehaviorSpecification extends Specification {

    @Shared
    ReactiveConsumer amqpConsumer;

    @Shared
    ProducerProxy producerProxy;

    @Shared
    Counter counter = new Counter();

    @Shared
    Counter consumedCounter = new Counter();

    def setupSpec() {
        amqpConsumer = mock(ReactiveConsumer.class)
        ReactiveAmqpProperties properties = defaultSetup()
        producerProxy = new ProducerProxy<>(getMockedRabbitTemplate(), properties)
        producerProxy = spy(producerProxy)
        mockSignalDemand()
        mockSourcePoll()
        mockSourceClose()
        mockSourceHasNext()
    }

    def void mockSignalDemand() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                fakeSource()
                return null
            }
        }).when(producerProxy).signalDemand()
    }

    def RabbitTemplate getMockedRabbitTemplate() {
        RabbitTemplate rabbitTemplate = mock(RabbitTemplate.class)
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                return null
            }
        }).when(rabbitTemplate).convertAndSend(anyString(),anyString(),any(Object.class))
        return rabbitTemplate
    }

    def mockSourcePoll() {
        doAnswer(new Answer<Map<String,Object>>() {
            @Override
            public Map<String,Object> answer(final InvocationOnMock invocation) throws Throwable {

                Map<String,Object> record = producerProxy.poll(5,TimeUnit.SECONDS)
                consumedCounter.inc()
                return record;
            }
        }).when(amqpConsumer).poll(anyInt(),any(TimeUnit.class))
    }

    def mockSourceClose() {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(final InvocationOnMock invocation) throws Throwable {
                consumedCounter.dec(consumedCounter.count)
                return null
            }
        }).when(amqpConsumer).close()
    }

    def mockSourceHasNext() {
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(final InvocationOnMock invocation) throws Throwable {
                return true
            }
        }).when(amqpConsumer).hasNext()
    }


    def fakeSource() {
        producerProxy.onMessage(generateEvents())
    }

    private Message generateEvents() {
        List<Map<String, Object>> records  = new ArrayList<>()
        MessageProperties messageProperties = new MessageProperties();
        10.times {
            Map<String,Object> record = new HashMap();
            counter.inc()
            record.put("sessionid",counter.getCount())
            record.put("time",System.currentTimeSeconds())
            messageProperties.setType(record.getClass().getTypeName())
            records.add(record)
        }
        return new Message(getBytes(records),messageProperties)
    }

    private byte[] getBytes(List<Map<String, Object>> records)  {
        try {
            ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
            return mapper.writeValueAsBytes(records);
        } catch (Exception e) {
            println e
        }
    }


    def ReactiveAmqpProperties defaultSetup() {
        ReactiveAmqpProperties properties = new ReactiveAmqpProperties();
        properties.setPublishBatchSize(10)
        properties.setAmqpBatchTimeout(10000)
        properties.setAmqpBatchSize(10)
        properties.setSerializationBatchSize(10)
        return properties
    }
}
