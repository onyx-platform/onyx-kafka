## onyx-kafka

Onyx plugin providing read and write facilities for Kafka.

#### Installation

In your project file:

```clojure
[com.mdrogalis/onyx-kafka "0.5.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.kafka])
```

#### Catalog entries

##### read-messages

```clojure
{:onyx/name :read-messages
 :onyx/ident :kafka/read-messages
 :onyx/type :input
 :onyx/medium :kafka
 :onyx/consumption :sequential
 :kafka/topic topic-name
 :kafka/zookeeper "127.0.0.1:2181"
 :kafka/group-id "onyx-consumer"
 :kafka/offset-reset "smallest"
 :onyx/batch-size batch-size
 :onyx/doc "Reads messages from a Kafka topic"}
```

##### write-messages

```clojure
{:onyx/name :write-messages
 :onyx/ident :kafka/write-messages
 :onyx/type :output
 :onyx/medium :kafka
 :onyx/consumption :concurrent
 :kafka/topic topic-name
 :kafka/brokers "127.0.0.1:9092"
 :kafka/serializer-class "kafka.serializer.DefaultEncoder"
 :kafka/partitioner-class "kafka.producer.DefaultPartitioner"
 :onyx/batch-size batch-size
 :onyx/doc "Reads messages from a Kafka topic"}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:kafka/topic`                | `string`  | The topic name to connect to
|`:kafka/zookeeper`            | `string`  | The ZooKeeper connection string
|`:kafka/group-id`             | `string`  | The consumer identity to store in ZooKeeper
|`:kafka/offset-reset`         | `string`  | Offset to seek to when not found - "smallest" or "largest"
|`:kafka/brokers`              | `string`  | A Kafka brokers connection string
|`:kafka/serializer-class`     | `string`  | The Kafka serialization class to use
|`:kafka/partitioner-class`    | `string`  | The Kafka partitioning class to use

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.