package com.rsa.reactive.rabbit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;


/**
 * This is a consumer proxy that deals with consumer interactions.
 *
 * @author Sudharshan Krishnamurthy
 */
public class ConsumerProxy implements Subscriber<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerProxy.class);

    Producer producerSubscription;

    @Override
    public void onSubscribe(Subscription producerSubscription) {
        this.producerSubscription = (Producer) producerSubscription;
    }

    @Override
    public void onNext(Message message) {
        producerSubscription.batchingRabbitTemplate.send(Main.EXCHANGE_NAME, "all", message);
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("Publishing ERROR: " + throwable.getMessage());
    }

    @Override
    public void onComplete() {
        producerSubscription.batchingRabbitTemplate.stop();
        producerSubscription.ctx.close();
        LOG.info("Publish Complete");
    }


    public void receive(boolean message) {
        if (message) {
            LOG.info("Received Response :" + message);
            producerSubscription.request(1);
        }
    }

}