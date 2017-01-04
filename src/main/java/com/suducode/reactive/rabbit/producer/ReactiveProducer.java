package com.suducode.reactive.rabbit.producer;

import com.codahale.metrics.Counter;
import com.suducode.reactive.rabbit.common.MetricsHelper;
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties;
import org.reactivestreams.Subscription;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.suducode.reactive.rabbit.common.LogMessages.TEXT;

/**
 * Reactive producer is used to publish events to the broker.
 *
 * Not a thread safe class.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ReactiveProducer<T> implements Subscription, Producer<T> {

    private static final Log LOG = LogFactory.getLog(ReactiveProducer.class);

    public static final String PRODUCER_PREFIX = "ReactiveProducer-";

    protected AtomicLong request = new AtomicLong(0);
    private AtomicBoolean cancel = new AtomicBoolean(false);
    private ConsumerProxy subscriber;
    private final ReactiveAmqpProperties producerProperties;
    private final ProducerConfiguration producerConfiguration;

    /**
     * Name of the metric of records processed
     */
    private static final String RECORDS_PROCESSED = "records.processed";

    private static final String METRIC_SCOPE = "amqp.producer";

    private Counter processedCounter;

    private String producerId;

    public ReactiveProducer(ReactiveAmqpProperties producerProperties) {

        this.producerProperties = producerProperties;
        producerId = PRODUCER_PREFIX + producerProperties.getId();

        // setup the amqp consumer infrastructure.
        producerConfiguration = new ProducerConfiguration(producerProperties);
        this.subscriber = producerConfiguration.getConsumerProxy();
        subscriber.onSubscribe(this);
        processedCounter = MetricsHelper.getInstance().getCounter(producerId, METRIC_SCOPE, RECORDS_PROCESSED);
        LOG.info(TEXT.startingAmqpProducer(producerId));
    }

    /**
     * Use this for default settings.
     */
    public ReactiveProducer() {
        this(new ReactiveAmqpProperties());
    }


    @Override
    public void request(long l) {
        request.set(l);
    }

    @Override
    public void cancel() {
        cancel.set(true);
    }


    @Override
    public boolean push(T data) {
        boolean success = false;
        if (request.get() > 0) {
            try {
                //send to consumer proxy
                subscriber.onNext(data);
                processedCounter.inc();
                success = true;

            } catch (Exception ex) {
                subscriber.onError(ex.getCause());
                subscriber.onComplete();
            }
        }
        return success;
    }


    @Override
    public void close() {
        producerConfiguration.cleanup();
        subscriber.onComplete();
        LOG.debug(TEXT.closingAmqpProducer(getId()));
    }

    @Override
    public String getId() {
        return producerId;
    }
}
