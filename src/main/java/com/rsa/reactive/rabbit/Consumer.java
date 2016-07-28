package com.rsa.reactive.rabbit;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;


/**
 * Consumer Initializer
 *
 * @author Sudharshan Krishnamurthy
 */

public class Consumer {

    private  ProducerProxy producerProxy;
    public Consumer() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(ConsumerConfiguration.class);
        ctx.refresh();
        producerProxy = (ProducerProxy) ctx.getBean("receiver");
    }

    // Mark the begining by sending the availability to receive messages.
    public void start() {
        producerProxy.sendFeedBack();
    }
}
