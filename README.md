# ReactiveRabbit
Provides reactive capability to rabbit mq messaging

Basic Model: Consumer drives the demand for the producer.


How does it work ?

When Producer comes up first , it waits for the signal from consumer for sending the first batch.
On receiving the demand signal , firstbatch gets published to message_exchange.
Consumer on receiving the entire batch sends the signal to reply_exchange indicating the readiness for the next batch.

How to run ?

Start Producer : java -jar reactive-rabbit-1.0-SNAPSHOT.jar producer
Start Consumer : java -jar reactive-rabbit-1.0-SNAPSHOT.jar consumer
 
Assumption :
Irrespective of the strategy, Message queue and Reply Queue would be visible to brokers that are part of the clustering.
