# ReactiveRabbit
Provides reactive capability to rabbit mq messaging

Basic Model: Consumer drives the demand for the producer.

Why ?

RabbitMq is popular messaging bus and used pretty extensively in the industry. While great at what it does, it does not offer
backpressure at a producer consumer level. It does provide backpressure using TCP (its underlying protocol) when the broker's memory fills up to
certain level, slowing down all the producers associated to it. This means one fast producer / slow consumer combination can slow down all producers
associated with the broker. By reactive capability of this library we provide backpresusre and as a nice side effect pretty good speed.
 The reason for speed comes from the rabbit queues being fairly empty for the most part of the data transfer.

How does it work ?

Producer awaits demand from the consumer via reply queue , upon receiving demand sends batches of messages to message queue upto the demand count for the
consumer to take, then stops and awaits for the next demand from the consumer.

