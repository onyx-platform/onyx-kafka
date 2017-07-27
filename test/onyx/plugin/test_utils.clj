(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [<! go-loop]]
            [com.stuartsierra.component :as component]
            [onyx.kafka.helpers :as h]
            [aero.core]
            [schema.core :as s]
            [taoensso.timbre :as log]))

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
   (h/create-topic! zk-address topic-name partitions 1)))

(defn write-data
  "Starts a Kafka in-memory instance, preloading a topic with xs.
   If xs is a channel, will load the topic with items off the channel."
  ([topic zookeeper xs]
   (let [config {"bootstrap.servers" ["127.0.0.1:9092"]}
         key-serializer (h/byte-array-serializer)
         value-serializer (h/byte-array-serializer)
         prod (h/build-producer config key-serializer value-serializer)]
     (if (sequential? xs)
       (do
         (doseq [x xs]
           (h/send-sync! prod topic 0 nil (.getBytes (pr-str x))))
         (.close prod))
       (go-loop [itm (<! xs)]
         (if itm
           (do (h/send-sync! prod topic 0 nil (.getBytes (pr-str itm)))
               (recur (<! xs)))
           (.close prod)))))))
