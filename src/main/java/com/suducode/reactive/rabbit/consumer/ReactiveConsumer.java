package com.suducode.reactive.rabbit.consumer;

import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.suducode.reactive.rabbit.common.LogMessages.TEXT;

/**
 * Consumer class is used to obtain the events from the broker.
 *
 * Not a thread safe class.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ReactiveConsumer<T> implements Consumer<T> {

    private static final String CONSUMER_PREFIX = "ReactiveConsumer-";

    private static final Log LOG = LogFactory.getLog(ReactiveConsumer.class);
    /**
     * The consumer configuration properties.
     */
    private ReactiveAmqpProperties consumerProperties;

    /**
     * A proxy for the AMQP processor.
     */
    private ProducerProxy<T> producerProxy;

    /**
     * Event count in the current batch.
     */
    private AtomicLong currentEventCount = new AtomicLong(0);

    /**
     * Agreed upon batch size between the amqp source and processor.
     */
    private long publishBatchSize;

    private String sourceId;

    /**
     * Amqp infrastructure configuration for consumer.
     */
    private ConsumerConfiguration consumerConfiguration;

    /**
     * Use this for custom settings
     * @param consumerProperties configuration properties
     */
    public ReactiveConsumer(ReactiveAmqpProperties consumerProperties) {

        this.consumerProperties = consumerProperties;
        this.publishBatchSize = consumerProperties.getPublishBatchSize();

        sourceId = CONSUMER_PREFIX + consumerProperties.getId();

        // setup the amqp consumer infrastructure.
        consumerConfiguration = new ConsumerConfiguration(consumerProperties);
        this.producerProxy = consumerConfiguration.getProducerProxy();

        // Signal demand to start the flow.
        producerProxy.signalDemand();
        LOG.info(TEXT.startingAmqpConsumer(sourceId));
    }

    /**
     * Use this for default settings.
     */
    public ReactiveConsumer() {
        this(new ReactiveAmqpProperties());
    }

    /**
     * Should always be true since we would be constantly listening for new records.
     *
     * @return true always
     */
    @Override
    public boolean hasNext() {
        return producerProxy.hasNext();
    }

    /**
     * Records are polled from the source.
     *
     * @param timeout The bounded wait time.
     * @param unit    The unit of the time.
     * @return a record.
     */
    @Override
    public T poll(int timeout, TimeUnit unit) {

        T record = null;
        try {
            record = producerProxy.poll(timeout, unit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (record != null) {
            advanceAggregation();
        }
        return record;
    }

    /**
     * Helps us advance the aggregation by sending the demand when needed.
     */
    void advanceAggregation() {
        long currentCount = currentEventCount.incrementAndGet();

        // Since with this we have officially consumed all the data for this batch, signal for next demand.
        if (currentCount == publishBatchSize) {
            producerProxy.signalDemand();
            currentEventCount.set(0);
        }
    }


    /**
     * Closes the source and the context.
     */
    @Override
    public void close() {
        producerProxy.cleanup();
        consumerConfiguration.cleanup();
        LOG.info(TEXT.closingAmqpConsumer(getId()));
    }

    @Override
    public String getId() {
        return sourceId;
    }

}
