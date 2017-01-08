(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [<! go-loop]]
            [com.stuartsierra.component :as component]
            [franzy.admin.topics :as k-topics]
            [franzy.admin.zookeeper.client :as k-admin]
            [aero.core]
            [franzy.clients.producer
             [client :as producer]
             [protocols :refer [send-sync!]]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [onyx.kafka.embedded-server :as ke]
            [schema.core :as s]
            [taoensso.timbre :as log])
  (:import franzy.clients.producer.types.ProducerRecord))

(defn read-config []
  (aero.core/read-config (clojure.java.io/resource "config.edn") 
                         {:profile (if (System/getenv "CIRCLECI")
                                     :ci
                                     :test)}))

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
    1)))

(defn write-data
  "Starts a Kafka in-memory instance, preloading a topic with xs.
   If xs is a channel, will load the topic with items off the channel."
  ([topic zookeeper xs]
   (let [config {:bootstrap.servers ["127.0.0.1:9092"]}
         key-serializer (byte-array-serializer)
         value-serializer (byte-array-serializer)
         prod (producer/make-producer config key-serializer value-serializer)]
     (if (sequential? xs)
       (do (doseq [x xs]
             (send-sync! prod (ProducerRecord. topic 0 nil (.getBytes (pr-str x)))))
           (.close prod))
       (go-loop [itm (<! xs)]
                (if itm
                  (do
                   (send-sync! prod (ProducerRecord. topic 0 nil (.getBytes (pr-str itm))))
                   (recur (<! xs)))
                  (.close prod)))))))
