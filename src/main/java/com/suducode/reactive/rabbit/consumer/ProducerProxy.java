package com.suducode.reactive.rabbit.consumer;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.suducode.reactive.rabbit.common.ReactiveAmqpProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkState;
import static com.suducode.reactive.rabbit.common.LogMessages.TEXT;

/**
 * Acts as a proxy for the AMQP producer. ReactiveConsumer handles all the interactions with the Producer using this
 * proxy.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */

public class ProducerProxy<T> implements MessageListener {

    private static final Log LOGGER = LogFactory.getLog(ProducerProxy.class);

    /**
     * A queue to transfer the batches between the amqp consumer thread and the stream subscription thread.
     */
    private BlockingQueue<T> incoming;

    /**
     * Exchange to which the next demand is sent.
     */
    private String replyExchangeName;

    /**
     * Routing key for the reply.
     */
    private String replyRoutingKey;

    /**
     * Routing key for the reply.
     */
    private String replyQueue;

    /**
     * Rabbit template to send the demand response.
     */
    private RabbitTemplate rabbitTemplate;

    /**
     * Source queue offer timeout.
     */
    private static final long OFFER_TIMEOUT_IN_SECONDS = 5000;

    /**
     * Unique source Identifier.
     */
    private String sourceId;

    /**
     * Stopwatch to keep a tab on supplied demand making it more fault tolerant.
     */
    private Stopwatch stopwatch;

    /**
     * Timeout parameter is a catchall for slow processing rates. When the processing
     * rates are slow and we are not filling the batch in stipulated time ,this kicks in and
     * completes the batch to be sent to the amqp broker.
     */
    private long amqpBatchTimeout;

    /**
     * When we issue a demand and we don't hear back from the producer for certain amount of time, greater than
     * AmqpBatchTimeout, giving it enough benefit of the doubt, then we should start our due diligence on what
     * might have happened. This service helps us do that.
     */
    private ScheduledExecutorService demandSupplyChecker;

    private static final long SCHEDULER_RATE = 10000;

    /**
     * Agreed upon batch size for issuing a demand by the consumer.
     */
    private int publishBatchSize;

    /**
     * An object mapper that helps with serialization.
     */
    private static ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    /**
     * What type of object are we expecting to deserialize ?
     */
    private Class<?> deserializationType;

    public ProducerProxy(RabbitTemplate rabbitTemplate, ReactiveAmqpProperties configuration) {

        this.rabbitTemplate = rabbitTemplate;
        this.sourceId = configuration.getId();
        this.replyExchangeName = configuration.getExchange();
        this.replyRoutingKey = ReactiveAmqpProperties.REPLY_ROUTING_KEY;
        this.replyQueue = replyExchangeName + ReactiveAmqpProperties.REPLY_QUEUE_SUFFIX;
        this.amqpBatchTimeout = configuration.getAmqpBatchTimeout();

        //Setup the Queue from which we will be pulling events.
        publishBatchSize = configuration.getPublishBatchSize();
        this.incoming = new ArrayBlockingQueue<>(publishBatchSize);

        demandSupplyChecker = Executors.newSingleThreadScheduledExecutor();
        demandSupplyChecker.scheduleAtFixedRate(new DemandSupplyCheckerThread(),
                SCHEDULER_RATE, SCHEDULER_RATE, TimeUnit.MILLISECONDS);
    }

    /**
     * Helper for sending the next demand for source.
     */
    public void signalDemand() {

        // Just make sure there is no existing demand before signaling demand.
        if (getReplyQueueSize() == 0) {
            rabbitTemplate.convertAndSend(replyExchangeName, replyRoutingKey, new Long(publishBatchSize));
            LOGGER.debug(TEXT.signalDemandForNextBatch(sourceId, replyExchangeName));

            //Start the watch
            startWatch();
        }
    }

    public boolean hasNext() {
        return !incoming.isEmpty();
    }

    /**
     * Reset the stop watch or start one if it doesnt exist.
     */
    private void resetStopWatch() {
        if (stopwatch != null) {

            // reset the watch if one exists.
            stopwatch.reset();
        }
    }

    /**
     * Reset the stop watch or start one if it doesnt exist.
     */
    private void startWatch() {

        // None exists so create one
        if (stopwatch == null) {
            stopwatch = Stopwatch.createStarted();
        } else {

            //if its running reset first.
            if (stopwatch.isRunning()) {
                resetStopWatch();
            }
            // if one already exists start the watch
            stopwatch.start();
        }
    }


    /**
     * Records receive handler that receives event from the AMQP broker and adds it to
     * the source queue.
     *
     * @param message received from the producer
     */
    @Override
    public void onMessage(Message message) {

        // we have received messages after supplying the demand, so reset the watch.
        resetStopWatch();
        try {
            //  if we dont know what the deserialization type as of yet then get it now !!
            if (deserializationType == null) {
                getDeserializationType(message);
            }
            List<T> data = deserializeMessage(message.getBody());
            data.forEach((event) -> {
                try {
                    offer(event);
                } catch (InterruptedException e) {
                    LOGGER.error(TEXT.errorOfferingToQueue(sourceId, e.getMessage()), e);
                }
            });
        } catch (Exception e) {
            LOGGER.error(TEXT.errorDeserializing(sourceId, e.getMessage()), e);
        }
    }

    /**
     * Get DeserializationType if not available already.
     */
    private void getDeserializationType(Message message) throws ClassNotFoundException {
        MessageProperties messageProperties = message.getMessageProperties();
        deserializationType = Class.forName(messageProperties.getType());
    }

    private void offer(T data) throws InterruptedException {
        boolean success = incoming.offer(data, OFFER_TIMEOUT_IN_SECONDS, TimeUnit.MILLISECONDS);
        checkState(success, "Offer to queue failed due to timeout.");
    }

    /**
     * Private helper utility to deserialize the objects received from AMQP broker.
     *
     * @param bytes to be deserialized.
     * @return List of records.
     * @throws IOException possible deserialization exception.
     */
    private List<T> deserializeMessage(byte[] bytes) throws IOException {
        JavaType expectedType = mapper.getTypeFactory().
                constructCollectionType(ArrayList.class, deserializationType);
        return mapper.readValue(bytes, expectedType);
    }

    /**
     * consumer polls from the consumer queue to which the receiver adds the records.
     *
     * @param timeOut  The bounded wait time.
     * @param timeUnit The unit of the time
     * @return A single record
     * @throws InterruptedException if we get an unexpected error.
     */
    public T poll(long timeOut, TimeUnit timeUnit) throws InterruptedException {
        return incoming.poll(timeOut, timeUnit);
    }

    /**
     * Gets the queue size for the reply queue.
     *
     * @return message count.
     */
    private int getReplyQueueSize() {
        AMQP.Queue.DeclareOk declareOk = rabbitTemplate.execute((Channel channel) ->
                channel.queueDeclarePassive(replyQueue)
        );
        return declareOk.getMessageCount();
    }


    /**
     * Thread that does the due diligence of what is going with the supply demand flow.
     * <p>
     * We wait for time greater than AmqpBatchTimeout to see if we receive any records. AmqpBatchTimeout
     * is used here because it is absolute latest we can expect a record to arrive since we issued a demand.
     * So if we are not getting records, couple of things might have happened.
     * <p>
     * 1. Producer might have consumed , gone down and come back up.
     * 2. Somebody might have accidentally deleted our messages.
     * <p>
     * If that is the case , resend the demand.
     */
    private class DemandSupplyCheckerThread implements Runnable {

        @Override
        public void run() {
            if (stopwatch != null && stopwatch.elapsed(TimeUnit.MILLISECONDS) > amqpBatchTimeout) {
                signalDemand();
            }
        }
    }

    protected void cleanup() {
        demandSupplyChecker.shutdown();
    }
}
