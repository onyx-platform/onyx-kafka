(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [<! go-loop]]
            [com.stuartsierra.component :as component]
            [franzy.admin.topics :as k-topics]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.clients.producer
             [client :as producer]
             [protocols :refer [send-sync!]]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [onyx.kafka.embedded-server :as ke]
            [schema.core :as s]
            [taoensso.timbre :as log])
  (:import franzy.clients.producer.types.ProducerRecord))

;; Set the log level, otherwise Kafka emits a huge amount
;; of messages.
(.setLevel (org.apache.log4j.LogManager/getRootLogger) org.apache.log4j.Level/WARN)

(s/defn create-topic
  ([zk-address topic-name] (create-topic zk-address topic-name 1))
  ([zk-address :- s/Str topic-name :- s/Str partitions :- s/Int]
   (log/info {:msg "Creating new topic"
              :zk-address zk-address
              :topic-name topic-name
              :partitions partitions})
   (k-topics/create-topic!
    (k-admin/make-zk-utils {:servers [zk-address]} false)
    topic-name
    partitions
    ;; Replication factor needs to be set because we're only running a single
    ;; embedded Kafka server.
    1
    )))

(defn mock-kafka
  "Starts a Kafka in-memory instance, preloading a topic with xs.
  If xs is a channel, will load the topic with items off the channel."
  ([topic zookeeper xs]
   (mock-kafka topic zookeeper xs (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))))
  ([topic zookeeper xs log-dir]
   (let [kafka-server (component/start
                       (ke/embedded-kafka {:advertised.host.name "127.0.0.1"
                                           :port 9092
                                           :broker.id 0
                                           :log.dir log-dir
                                           :zookeeper.connect zookeeper
                                           :controlled.shutdown.enable false}))
         _ (create-topic zookeeper topic)
         config {:bootstrap.servers ["127.0.0.1:9092"]}
         key-serializer (byte-array-serializer)
         value-serializer (byte-array-serializer)
         prod (producer/make-producer config key-serializer value-serializer)]
     (if (sequential? xs)
       (doseq [x xs]
         (send-sync! prod (ProducerRecord. topic 0 nil (.getBytes (pr-str x)))))
       (go-loop [itm (<! xs)]
         (if itm
           (do
             (send-sync! prod (ProducerRecord. topic 0 nil (.getBytes (pr-str itm))))
             (recur (<! xs))))))
     kafka-server)))
