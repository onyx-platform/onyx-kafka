[//]: # ({:display :header, :valid-structure? true, :rendered-params ({:display :summary, :model :onyx.plugin.kafka/read-messages, :format :h6} {:display :attribute-table, :model :onyx.plugin.kafka/read-messages, :columns [[:key "Key"] [:type "Type"] [:default "Default" :code] [:doc "Description"]]} {:display :catalog-entry, :model :onyx.plugin.kafka/read-messages, :merge-additions {}} {:display :lifecycle-entry, :model :onyx.plugin.kafka/read-messages, :merge-additions {}} {:display :summary, :model :onyx.plugin.kafka/write-messages, :format :h6} {:display :catalog-entry, :model :onyx.plugin.kafka/write-messages, :merge-additions {}} {:display :attribute-table, :model :onyx.plugin.kafka/write-messages, :columns [[:key "Key"] [:type "Type"] [:default "Default" :code] [:doc "Description"]]} {:display :lifecycle-entry, :model :onyx.plugin.kafka/write-messages, :merge-additions {}})})
[//]: # (✔ All catalog entries documented.)
[//]: # (✔ All lifecycle entries documented.)
## onyx-kafka

Onyx plugin providing read and write facilities for Kafka. This plugin automatically discovers broker locations from ZooKeeper and updates the consumers when there is a broker failover.

This plugin version is *only compatible with Kafka 0.9+*. Please use [onyx-kafka-0.8](https://github.com/onyx-platform/onyx-kafka-0.8) with Kafka 0.8.

### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-kafka "0.9.11.2-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.kafka])
```

### Task Bundles

#### `:onyx.plugin.kafka/read-messages`

[//]: # ({:display :summary, :model :onyx.plugin.kafka/read-messages, :format :h6})
###### An input task to read messages from a Kafka topic.

[//]: # ({:display :attribute-table, :model :onyx.plugin.kafka/read-messages, :columns [[:key "Key"] [:type "Type"] [:default "Default" :code] [:doc "Description"]]})

| Key                           | Type       | Default  | Description                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------ | ---------- | -------- | -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `:kafka/topic`                | `:string`  |          | The topic name to read from.                                                                                                                                                                                                                                                                                                                                     |
| `:kafka/partition`            | `:string`  |          | Partition to read from if auto-assignment is not used.                                                                                                                                                                                                                                                                                                           |
| `:kafka/group-id`             | `:string`  |          | The consumer identity to store in ZooKeeper.                                                                                                                                                                                                                                                                                                                     |
| `:kafka/zookeeper`            | `:string`  |          | The ZooKeeper connection string.                                                                                                                                                                                                                                                                                                                                 |
| `:kafka/offset-reset`         | `:keyword` |          | Offset bound to seek to when not found - `:earliest` or `:latest`.                                                                                                                                                                                                                                                                                               |
| `:kafka/force-reset?`         | `:boolean` |          | Force to read from the beginning or end of the log, as specified by `:kafka/offset-reset`. If false, reads from the last acknowledged messsage if it exists.                                                                                                                                                                                                     |
| `:kafka/deserializer-fn`      | `:keyword` |          | A keyword that represents a fully qualified namespaced function to deserialize a message. Takes one argument, which must be a byte array.                                                                                                                                                                                                                        |
| `:kafka/receive-buffer-bytes` | `:long`    | `65536`  | The size in the receive buffer in the Kafka consumer.                                                                                                                                                                                                                                                                                                            |
| `:kafka/commit-interval`      | `:long`    | `2000`   | The interval in milliseconds to commit the latest acknowledged offset to ZooKeeper.                                                                                                                                                                                                                                                                              |
| `:kafka/wrap-with-metadata?`  | `:boolean` | `false`  | Wraps message into map with keys `:offset`, `:partitions`, `:topic` and `:message` itself.                                                                                                                                                                                                                                                                       |
| `:kafka/start-offsets`        | `:map`     |          | Allows a task to be supplied with the starting offsets for all partitions. Maps partition to offset, e.g. `{0 50, 1, 90}` will start at offset 50 for partition 0, and offset 90 for partition 1.                                                                                                                                                                |
| `:kafka/consumer-opts`        | `:map`     |          | A map of arbitrary configuration to merge into the underlying Kafka consumer base configuration. Map should contain keywords as keys, and the valid values described in the [Kafka Docs](http://kafka.apache.org/documentation.html#newconsumerconfigs). Please note that key values such as `fetch.min.bytes` must be in keyword form, i.e. `:fetch.min.bytes`. |
| `:kafka/empty-read-back-off`  | `:long`    | `500`    | The amount of time to back off between reads when nothing was fetched from a consumer.                                                                                                                                                                                                                                                                           |
| `:kafka/fetch-size`           | `:long`    | `307200` | The size in bytes to request from ZooKeeper per fetch request.                                                                                                                                                                                                                                                                                                   |
| `:kafka/chan-capacity`        | `:long`    | `1000`   | The buffer size of the Kafka reading channel.                                                                                                                                                                                                                                                                                                                    |


[//]: # ({:display :catalog-entry, :model :onyx.plugin.kafka/read-messages, :merge-additions {}})
```clojure
{:kafka/topic "The topic name to read from.",
 :kafka/partition "Partition to read from if auto-assignment is not used.",
 :kafka/group-id "The consumer identity to store in ZooKeeper.",
 :kafka/zookeeper "The ZooKeeper connection string.",
 :kafka/offset-reset :earliest,
 :kafka/force-reset? false,
 :kafka/deserializer-fn :my.ns/deserializer-fn,
 :kafka/receive-buffer-bytes 65536,
 :kafka/commit-interval 2000,
 :kafka/wrap-with-metadata? false,
 :kafka/start-offsets :onyx.gen-doc/please-handle-in-merge-additions,
 :kafka/consumer-opts :onyx.gen-doc/please-handle-in-merge-additions}
```

[//]: # ({:display :lifecycle-entry, :model :onyx.plugin.kafka/read-messages, :merge-additions {}})
```clojure
[{:task.lifecycle/name :read-messages,
  :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]
```

#### `:onyx.plugin.kafka/write-messages`

[//]: # ({:display :summary, :model :onyx.plugin.kafka/write-messages, :format :h6})
###### Write messages to kafka.

[//]: # ({:display :catalog-entry, :model :onyx.plugin.kafka/write-messages, :merge-additions {}})
```clojure
{:kafka/topic
 "The topic name to write to. Must either be supplied or otherwise all messages must contain a `:topic` key",
 :kafka/zookeeper "The ZooKeeper connection string.",
 :kafka/partition
 "Partition to write to, if you do not wish messages to be auto allocated to partitions. Must either be supplied in the task map, or all messages should contain a `:partition` key.",
 :kafka/serializer-fn :my.ns/serializer-fn,
 :kafka/request-size :onyx.gen-doc/please-handle-in-merge-additions,
 :kafka/no-seal? false,
 :kafka/producer-opts :onyx.gen-doc/please-handle-in-merge-additions}
```

[//]: # ({:display :attribute-table, :model :onyx.plugin.kafka/write-messages, :columns [[:key "Key"] [:type "Type"] [:default "Default" :code] [:doc "Description"]]})

| Key                    | Type       | Default | Description                                                                                                                                                                                                                                                                                                                                               |
|----------------------- | ---------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `:kafka/topic`         | `:string`  |         | The topic name to write to. Must either be supplied or otherwise all messages must contain a `:topic` key                                                                                                                                                                                                                                                 |
| `:kafka/zookeeper`     | `:string`  |         | The ZooKeeper connection string.                                                                                                                                                                                                                                                                                                                          |
| `:kafka/partition`     | `:string`  |         | Partition to write to, if you do not wish messages to be auto allocated to partitions. Must either be supplied in the task map, or all messages should contain a `:partition` key.                                                                                                                                                                        |
| `:kafka/serializer-fn` | `:keyword` |         | A keyword that represents a fully qualified namespaced function to serialize a message. Takes one argument - the segment.                                                                                                                                                                                                                                 |
| `:kafka/request-size`  | `:long`    |         | The maximum size of request messages.  Maps to the `max.request.size` value of the internal kafka producer.                                                                                                                                                                                                                                               |
| `:kafka/no-seal?`      | `:boolean` | `false` | Do not write :done to the topic when task receives the sentinel signal (end of batch job).                                                                                                                                                                                                                                                                |
| `:kafka/producer-opts` | `:map`     |         | A map of arbitrary configuration to merge into the underlying Kafka producer base configuration. Map should contain keywords as keys, and the valid values described in the [Kafka Docs](http://kafka.apache.org/documentation.html#producerconfigs). Please note that key values such as `buffer.memory` must be in keyword form, i.e. `:buffer.memory`. |


[//]: # ({:display :lifecycle-entry, :model :onyx.plugin.kafka/write-messages, :merge-additions {}})
```clojure
[{:task.lifecycle/name :write-messages,
  :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}]
```

#### Test Utilities

A take-segments utility function is provided for use when testing the results
of jobs with kafka output tasks. take-segments reads from a topic until a :done
is reached, and then returns the results. Note, if a `:done` is never written to a
topic, this will hang forever as there is no timeout.

```clojure
(ns your-ns.a-test
  (:require [onyx.kafka.utils :as kpu]))

;; insert code to run a job here

;; retrieve the segments on the topic
(def results
  (kpu/take-segments (:zookeeper/addr peer-config) "yourtopic" your-decompress-fn))

(last results)
; :done

```

#### Embedded Kafka Server

An embedded Kafka server is included for use in test cases where jobs output to
kafka output tasks. Note, stopping the server will *not* perform a [graceful shutdown](http://kafka.apache.org/documentation.html#basic_ops_restarting) -
please do not use this embedded server for anything other than tests.

This can be used like so:

```clojure
(ns your-ns.a-test
  (:require [onyx.kafka.embedded-server :as ke]
            [com.stuartsierra.component :as component]))

(def kafka-server
  (component/start
    (ke/map->EmbeddedKafka {:hostname "127.0.0.1"
                            :port 9092
                            :broker-id 0
			    :num-partitions 1
			    ; optional log dir name - randomized dir will be created if none is supplied
			    ; :log-dir "/tmp/embedded-kafka"
			    :zookeeper-addr "127.0.0.1:2188"})))

;; insert code to run a test here

;; stop the embedded server
(component/stop kafka-server)

```

#### Development

To benchmark, start a real ZooKeeper instance (at 127.0.0.1:2181) and Kafka instance, and run the following benchmarks.

Write perf, single peer writer:
```
TIMBRE_LOG_LEVEL=info lein test onyx.plugin.output-bench-test :benchmark
```

Read perf, single peer reader:
```
TIMBRE_LOG_LEVEL=info lein test onyx.plugin.input-benchmark-test :benchmark
```

Past results are maintained in `dev-resources/benchmarking/results.txt`.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry

Distributed under the Eclipse Public License, the same as Clojure.
