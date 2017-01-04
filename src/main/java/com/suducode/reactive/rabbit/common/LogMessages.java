package com.suducode.reactive.rabbit.common;
        
import static java.text.MessageFormat.format;

/**
 * Messages catalog for AMQP consumers and producers.
 *
 * @author Sudharshan Krishnamurthy
 * @version 1.0
 */
public enum LogMessages {

    TEXT;

    public String startingAmqpProducer(String producer) {
        return format("AMQP producer {0} started", producer);
    }

    public String closingAmqpProducer(String producer) {
        return format("Closing AMQP producer {0}", producer);
    }

    public String signalDemandForNextBatch(String consumer, String replyExchange) {
        return format("AMQP consumer {0} signalling demand for next batch to exchange {1}.", consumer, replyExchange);
    }

    public String errorOfferingToQueue(String consumer, String error) {
        return format("Unable to offer records to the buffer queue on AMQP consumer {0} : {1}", consumer, error);
    }

    public String errorProcessingRecord(String producer, String error) {
        return format("Unable to process records on AMQP producer {0} : {1}", producer, error);
    }

    public String errorDeserializing(String consumer, String error) {
        return format("Error de-serializing records on AMQP consumer {0} : {1}", consumer, error);
    }

    public String errorSerializing(String producer) {
        return format("Error serializing records on AMQP producer {0}.", producer);
    }

    public String startingAmqpConsumer(String consumer) {
        return format("AMQP consumer {0} started", consumer);
    }

    public String closingAmqpConsumer(String consumer) {
        return format("Closing AMQP consumer {0}", consumer);
    }

    public String producerBatchComplete(String producer) {
        return format("AMQP producer {0} has sent the current batch to broker and waiting for the next demand",
                producer);
    }

    public String producerSerializationBatchComplete(String producer, int batchSize) {
        return format("AMQP producer {0} completed serializing batch of size {1} to send to broker.",
                producer, batchSize);
    }

    public String producerStatusUpdateThreadWaiting(String producer) {
        return format("AMQP producer {0} status updater thread waiting to update the process state.", producer);
    }

    public String unExpectedError(String error) {
        return format("Encountered Unexpected error {0}", error);
    }
}

