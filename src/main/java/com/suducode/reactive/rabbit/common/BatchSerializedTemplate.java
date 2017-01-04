package com.suducode.reactive.rabbit.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.suducode.reactive.rabbit.producer.ReactiveProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.scheduling.TaskScheduler;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.suducode.reactive.rabbit.common.LogMessages.TEXT;

/**
 * There are 2 levels of batching happening here
 * a. Serialize data in batches.
 * b. Batch send the already serialized data (batched) to broker.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public class BatchSerializedTemplate extends BatchingRabbitTemplate {

    private static final Log LOGGER = LogFactory.getLog(BatchSerializedTemplate.class);

    private final ReactiveAmqpProperties configProperties;

    /**
     * Batch size used for serialization before publishing to the broker.
     */
    private int serializationBatchSize;

    /**
     * Maximum amount of time, in milliseconds, to wait before creating a batch for serialization.
     * At the timeout, the batch will be serialized regardless of the number of messages.
     */
    private long serializationBatchTimeout;

    /**
     * Publish batch size agreed upon by the amqp processor and source.
     */
    private int publishBatchSize;


    /**
     * List that determines the batch size for serialization.
     */
    private ArrayList serializationBatch = new ArrayList<>();

    /**
     * A counter for the number of records that are being serialized in this batch.
     */
    private AtomicInteger serializationCount = new AtomicInteger(0);

    /**
     * Stopwatch to keep a tab on serialization batch formation.
     */
    private Stopwatch stopwatch;

    /**
     * A counter for the number of records processed in this batch.
     */
    private AtomicLong batchProcessedCount = new AtomicLong(0);

    /**
     * An object mapper that helps with serialization.
     */
    private static ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    /**
     * Message exchange name
     */
    private String messageExchange;

    /**
     * Message routing key
     */
    private String messageRoutingKey;


    /**
     * Unique producer Identifier.
     */
    private String producerId;

    private Class<?> type;

    public BatchSerializedTemplate(BatchingStrategy batchingStrategy, TaskScheduler scheduler, ReactiveAmqpProperties configProperties) {
        super(batchingStrategy, scheduler);
        this.configProperties = configProperties;
        this.publishBatchSize = configProperties.getPublishBatchSize();
        this.serializationBatchSize = configProperties.getSerializationBatchSize();
        this.serializationBatchTimeout = configProperties.getSerializationBatchTimeout();
        this.messageExchange = configProperties.getExchange();
        this.messageRoutingKey = ReactiveAmqpProperties.MESSAGE_ROUTING_KEY;
        this.producerId = ReactiveProducer.PRODUCER_PREFIX + configProperties.getId();
    }

    public boolean send(Object record) throws JsonProcessingException {

        // Send the type information to the consumer for deserialization.
        if (type == null) {
            type = record.getClass();
        }

        boolean batchComplete = false;

        // Get message to be published to Amqp broker.
        Message message = getBatchedMessage(record);

        if (message != null) {

            // Send it broker and get an incremented count if successful.
            long numberOfRecordsSentToBroker = sendMessageToBroker(message);

            //now that we have successfully sent the serialized batch to the broker reset the batch.
            resetSerializationBatch();

            // Marks the end of the batch, so stop the processing till we get a go from consumer.
            if (numberOfRecordsSentToBroker == publishBatchSize) {

                // reset the processed count for the next batch
                resetProcessedCount();

                // log that we are done with the batch
                LOGGER.debug(TEXT.producerBatchComplete(producerId));

                batchComplete = true;
            }
        }
        return batchComplete;
    }


    protected void resetSerializationBatch() {
        serializationBatch.clear();
        setSerializationCount(0);
    }

    protected void resetProcessedCount() {
        batchProcessedCount.set(0);
    }

    protected void setSerializationCount(int value) {
        serializationCount.set(value);
    }


    /**
     * Gets a message object that is batch serialized whose size
     * is determined by {@link #serializationBatchSize}.
     *
     * @param record to be serialized
     * @return Batched Message that is serialized or null if batch not complete.
     */
    protected Message getBatchedMessage(Object record) throws JsonProcessingException {
        Message message = null;
        boolean isTimeUp = checkElapsedTime();
        serializationBatch.add(record);
        serializationCount.incrementAndGet();

        //Check if we hit the batch size or the timeout
        if (serializationBatch.size() == serializationBatchSize || isTimeUp) {
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setType(type.getTypeName());
            message = new Message(getBytes(serializationBatch), messageProperties);
            LOGGER.debug(TEXT.producerSerializationBatchComplete(producerId,
                    serializationBatch.size()));
        }
        return message;
    }

    /**
     * Stopwatch to keep track of time taken to formulate a serialization batch. If batch is not
     * filled in stipulated time dictated by the {@link #serializationBatchTimeout} we will serialize
     * what we have.
     *
     * @return status true or false
     */
    private boolean checkElapsedTime() {

        // None exists so create one
        if (stopwatch == null) {
            stopwatch = Stopwatch.createStarted();
        } else if (serializationBatch.isEmpty()) {

            // Its time to reset the watch with a new batch
            stopwatch.reset();
            stopwatch.start();
        }
        return stopwatch.elapsed(TimeUnit.MILLISECONDS) >= serializationBatchTimeout;
    }


    /**
     * Send message to the broker and increment the processed counter
     *
     * @param message batched message after serialization
     * @return processed count
     */
    protected long sendMessageToBroker(Message message) {
        send(messageExchange, messageRoutingKey, message);
        return batchProcessedCount.addAndGet(serializationCount.get());
    }


    /**
     * private helper for serialization
     *
     * @param records list of records.
     * @return serialized byte array to be sent to amqp broker.
     */
    private byte[] getBytes(ArrayList records) throws JsonProcessingException {
        return mapper.writeValueAsBytes(records);
    }
}
