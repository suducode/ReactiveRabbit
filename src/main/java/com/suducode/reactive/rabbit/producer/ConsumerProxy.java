package com.suducode.reactive.rabbit.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.suducode.reactive.rabbit.common.BatchSerializedTemplate;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.suducode.reactive.rabbit.common.LogMessages.TEXT;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is a consumer proxy that deals with AMQP broker interactions pertaining to consumer.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class ConsumerProxy implements Subscriber {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerProxy.class);

    /**
     * Producer Subscription.
     */
    private ReactiveProducer producerSubscription;

    /**
     * Custom Batched template used to send data to broker.
     */
    private BatchSerializedTemplate batchSerializedTemplate;

    /**
     * An Executor service that helps us update the process status.
     */
    private ExecutorService statusUpdateService = Executors.newSingleThreadExecutor();


    public ConsumerProxy(BatchSerializedTemplate batchSerializedTemplate) {
        this.batchSerializedTemplate = batchSerializedTemplate;
    }

    @Override
    public void onSubscribe(Subscription producerSubscription) {
        checkArgument(producerSubscription instanceof  ReactiveProducer, "Not a valid Subscription");
        this.producerSubscription = (ReactiveProducer) producerSubscription;
    }

    @Override
    public void onNext(Object message) {
        try {
           if (batchSerializedTemplate.send(message)) {
               // we need more demand to process further.
               producerSubscription.request(0);
           }
        } catch (JsonProcessingException e) {
            onError(e.getCause());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("Publishing ERROR: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        batchSerializedTemplate.stop();
        producerSubscription.close();
        cleanup();
        LOG.info("Data Publishing Complete !!");
    }

    /**
     * Receives the demand signal through the demand queue.
     *
     * Note: Special method used by amqp so no blocking operations.
     *
     * @param demand next demand.
     */
    public void receive(long demand) {
        statusUpdateService.submit(new ChangeStatus(demand));
    }

    /**
     * A thread that helps us update our process status when ready.
     */
    private class ChangeStatus implements Runnable {
        long demand = 0;

        ChangeStatus(long demand) {
            this.demand = demand;
        }

        @Override
        public void run() {

            /*
             * Don't update the demand unless the previous unit of work was marked done which
             * in this case is having process demand to be 0.
             */
            while (!producerSubscription.request.compareAndSet(0, demand)) {
                LOG.debug(TEXT.producerStatusUpdateThreadWaiting(producerSubscription.getId()));
            }
        }
    }

    /**
     * Clean up the status update thread and the status.
     */
    protected void cleanup() {
        producerSubscription.request(0);
        statusUpdateService.shutdown();
    }

}