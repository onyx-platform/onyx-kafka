(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [go-loop <!! chan <! >!]]
            [com.stuartsierra.component :as component]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.cluster :as k-cluster]
            [franzy.admin.topics :as k-topics]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.protocols :refer [send-sync!]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [onyx.kafka.embedded-server :as ke])
  (:import [franzy.clients.producer.types ProducerRecord]))

;; Set the log level, otherwise Kafka emits a huge amount
;; of messages.
(.setLevel (org.apache.log4j.LogManager/getRootLogger) org.apache.log4j.Level/WARN)

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
         zk-utils (k-admin/make-zk-utils {:servers [zookeeper]} false)
         _ (k-topics/create-topic! zk-utils topic 1)
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
