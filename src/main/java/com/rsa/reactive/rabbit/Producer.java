package com.rsa.reactive.rabbit;

import com.codahale.metrics.Meter;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data Producer.
 *
 * @author Sudharshan Krishnamurthy
 */
public class Producer implements Subscription {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private AtomicLong request = new AtomicLong(0);
    private AtomicBoolean cancel = new AtomicBoolean(false);
    protected final static int BATCH_SIZE = 100000;
    protected BatchingRabbitTemplate batchingRabbitTemplate;
    protected AnnotationConfigApplicationContext ctx;
    private ConsumerProxy subscriber;
    private Meter meter;
    private ProducerProperties properties;

    private final static long TOTAL_SIZE = 2000000;

    public Producer() {
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(ProducerConfiguration.class);
        ctx.refresh();

        batchingRabbitTemplate = (BatchingRabbitTemplate) ctx.getBean("batchingRabbitTemplate");
        subscriber = (ConsumerProxy) ctx.getBean("receiver");
        properties = (ProducerProperties) ctx.getBean("properties");
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long l) {
        request.set(l);
    }

    @Override
    public void cancel() {
        cancel.set(true);
    }

    private void displayMeter() {
        LOG.info("Count {}, mean rate {}, 1 min avg {}, 5 min avg {}, 15 min avg {}", meter.getCount(),
                meter.getMeanRate(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(), meter.getFifteenMinuteRate());
    }

    public void start() {
        Message message = new Message(new byte[properties.getMessageSize()], new MessageProperties());
        meter = new Meter();
        long counter = 0;
        try {
            while (meter.getCount() < TOTAL_SIZE) {
                if (request.get() > 0 && !cancel.get()) {
                    if (counter < BATCH_SIZE) {
                        subscriber.onNext(message);
                        meter.mark();
                        counter++;
                        if (meter.getCount() % 100_000 == 0) {
                            displayMeter();
                            request.set(0);
                            batchingRabbitTemplate.flush();
                            counter = 0;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            subscriber.onError(ex.getCause());
        } finally {
            subscriber.onComplete();
        }
    }
}
