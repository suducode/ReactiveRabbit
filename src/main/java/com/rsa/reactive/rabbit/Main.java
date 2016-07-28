package com.rsa.reactive.rabbit;


/**
 * Main driver
 *
 * @author Sudharshan Krishnamurthy
 */

public class Main {

    public final static String QUEUE_NAME = "message_queue";
    public final static String EXCHANGE_NAME = "message_exchange";

    public final static String REPLY_QUEUE_NAME = "reply_queue";
    public final static String REPLY_EXCHANGE_NAME = "reply_exchange";


    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase("consumer")) {
            Consumer consumer = new Consumer();
            consumer.start();
        } else if (args[0].equalsIgnoreCase("producer")) {
            Producer producer = new Producer();
            producer.start();
        }
    }
}
