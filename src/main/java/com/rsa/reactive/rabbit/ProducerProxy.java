package com.rsa.reactive.rabbit;

import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * This is a producer proxy that deals with producer interactions.
 *
 * @author Sudharshan Krishnamurthy
 */
public class ProducerProxy implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerProxy.class);

    private Meter meter;

    private RabbitTemplate rabbitTemplate;

    public ProducerProxy(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.meter = new Meter();
    }

    public void sendFeedBack() {
        rabbitTemplate.convertAndSend(Main.REPLY_EXCHANGE_NAME, "all", true);
        LOG.info("Sending Response");
    }

    private void displayMeter() {
        LOG.info("Count {}, mean rate {}, 1 min avg {}, 5 min avg {}, 15 min avg {}", meter.getCount(),
                meter.getMeanRate(), meter.getOneMinuteRate(), meter.getFiveMinuteRate(), meter.getFifteenMinuteRate());
    }

    @Override
    public void onMessage(Message message) {
        meter.mark();
        if (meter.getCount() % 100_000 == 0) {
            displayMeter();
            sendFeedBack();
        }
    }
}
