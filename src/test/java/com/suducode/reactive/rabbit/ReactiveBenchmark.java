package com.suducode.reactive.rabbit;

import com.codahale.metrics.Meter;
import com.suducode.reactive.rabbit.consumer.Consumer;
import com.suducode.reactive.rabbit.consumer.ReactiveConsumer;
import com.suducode.reactive.rabbit.producer.Producer;
import com.suducode.reactive.rabbit.producer.ReactiveProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Library benchmark.
 *
 * @author sudharshan krishnamurthy.
 */
public class ReactiveBenchmark {

    static Meter meter = new Meter();

    static int numberOfEvents = 10000000;

    public static void main(String[] args) {

        ExecutorService service = Executors.newFixedThreadPool(2);

        service.submit(new ConsumerImpl());
        service.submit(new ProducerImpl());

        while (meter.getCount() < numberOfEvents + 1) ;

        service.shutdownNow();
    }

    private static class ProducerImpl implements Runnable {

        @Override
        public void run() {
            Producer<Map<String, Object>> producer = new ReactiveProducer<>();
            int i = 0;
            while (i < numberOfEvents) {
                Map<String, Object> data = new HashMap();
                data.put("id", i);
                data.put("payload", new byte[500]);
                while (!producer.push(data));
                i++;
            }
            while (meter.getCount() < numberOfEvents) ;
           producer.close();
        }
    }

    private static class ConsumerImpl implements Runnable {
        @Override
        public void run() {
            Consumer<Map<String, Object>> consumer = new ReactiveConsumer<>();
            while (meter.getCount() < numberOfEvents) {
                Map<String, Object> test = consumer.poll(5, TimeUnit.MILLISECONDS);
                if (test != null) {
                    meter.mark();
                    if (meter.getCount() % 100000 == 0) {
                        System.out.println("Count : " + meter.getCount() + " Mean rate: " + meter.getMeanRate());
                    }
                }
            }
            meter.mark();
            consumer.close();
        }
    }
}
