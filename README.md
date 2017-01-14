# ReactiveRabbit
Provides reactive capability to rabbit mq messaging

# Basic Model: 

Consumer drives the demand for the producer.

# Why ?

RabbitMq is popular messaging bus and used pretty extensively in the industry. While great at what it does, it does not offer
backpressure at a single producer consumer level. It does provide backpressure using TCP (its underlying protocol) when the broker's memory fills up to certain level, slowing down all the producers associated to it. This means one fast producer / slow consumer combination can slow down all producers associated with the broker.But with this reactive library we provide backpresusre at a single producer/ consumer level and as a nice side effect, we get pretty good speed.The speed comes from the fact that queues are fairly empty for the most part of the data transfer making it a pass through.

# How does it work ?

Producer awaits demand from the consumer via reply queue.Upon receiving demand, producer sends batches of messages upto the size of demand, to the message queue for the consumer to take and stops, awaiting the next demand from the consumer.

# How do you use it ?

# Producer :

`Producer<Map<String, Object>> producer = new ReactiveProducer<>();`

`Map<String, Object> data = new HashMap();`

producer.push(data) -- > This returns true or false depending on demand availability and the client will have to handle the false scenario.

# Consumer: 

`Consumer<Map<String, Object>> consumer = new ReactiveConsumer<>();`

`Map<String, Object> test = consumer.poll(5, TimeUnit.MILLISECONDS);`

# Benchmark Results 

On my old mac with 54 mbit/s link 

Average records per second : 115 - 120 k

record size : ~ 600 MB


