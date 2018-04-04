# kafka-flink-jobs

This project provides a few simle samples Kafka Flink jobs that a user can build on to get a better understanding of how the technologies work together.

* Text Stream Consumer and Producer
* Transaction Stream Consumer and Producer (build your own operations and transformation)
* Wiki Stream Consumer (Consumer Only; does not use Kafka and only Flink w/a real live stream of data)

The jobs are setup to be run against a locally running instance of Kafka (version 2.11) and each consumer/producer has an example {{mvn}} command line argument to get them started.
