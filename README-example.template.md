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

```onyx-gen-doc
{:display :summary
 :model :onyx.plugin.kafka/read-messages
 :format :h6}
```

```onyx-gen-doc
{:display :attribute-table
 :model :onyx.plugin.kafka/read-messages
 :columns [[:key "Key"] [:type "Type"] [:default "Default"] [:doc "Description"]]}
```

```onyx-gen-doc
{:display :catalog-entry
 :model :onyx.plugin.kafka/read-messages
 :merge-additions {}}
```

```onyx-gen-doc
{:display :lifecycle-entry
 :model :onyx.plugin.kafka/read-messages
 :merge-additions {}}
```

#### `:onyx.plugin.kafka/write-messages`

```onyx-gen-doc
{:display :summary
 :model :onyx.plugin.kafka/write-messages
 :format :h6}
```

```onyx-gen-doc
{:display :catalog-entry
 :model :onyx.plugin.kafka/write-messages
 :merge-additions {}}
```

```onyx-gen-doc
{:display :attribute-table
 :model :onyx.plugin.kafka/write-messages
 :columns [[:key "Key"] [:type "Type"] [:default "Default"] [:doc "Description"]]}
```

```onyx-gen-doc
{:display :lifecycle-entry
 :model :onyx.plugin.kafka/write-messages
 :merge-additions {}}
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
