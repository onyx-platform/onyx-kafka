## onyx-kafka

Onyx plugin providing read and write facilities for Kafka.

#### Installation

In your project file:

```clojure
[com.mdrogalis/onyx-kafka "0.6.0-alpha2"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.kafka])
```

#### Functions

##### read-messages

Reads segments from a Kafka topic.

Catalog entry:

```clojure
{:onyx/name :read-messages
 :onyx/ident :kafka/read-messages
 :onyx/type :input
 :onyx/medium :kafka
 :kafka/topic "topic-name"
 :kafka/zookeeper "127.0.0.1:2181"
 :kafka/group-id "onyx-consumer"
 :kafka/offset-reset "smallest"
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads messages from a Kafka topic"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-messages
 :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
```

##### write-messages

Writes segments to a Kafka topic.

Catalog entry:

```clojure
{:onyx/name :write-messages
 :onyx/ident :kafka/write-messages
 :onyx/type :output
 :onyx/medium :kafka
 :kafka/topic "topic"
 :kafka/brokers "127.0.0.1:9092"
 :kafka/serializer-class "kafka.serializer.DefaultEncoder"
 :kafka/partitioner-class "kafka.producer.DefaultPartitioner"
 :onyx/batch-size batch-size
 :onyx/doc "Writes messages to a Kafka topic"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-messages
 :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}
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

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.