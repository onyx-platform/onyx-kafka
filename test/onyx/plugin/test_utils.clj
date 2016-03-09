(ns onyx.plugin.test-utils
  (:require [com.stuartsierra.component :as component]
            [clojure.core.async :refer [go-loop <!! chan <! >!]]
            [onyx.kafka.embedded-server :as ke]
            [clj-kafka.admin :as kadmin]
            [clj-kafka.producer :as kp]))

(defn mock-kafka
  "Starts a Kafka in-memory instance, preloading a topic with xs.
  If xs is a channel, will load the topic with items off the channel."
  ([topic zookeeper xs]
   (mock-kafka topic zookeeper xs (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))))
  ([topic zookeeper xs log-dir]
   (let [kafka-server (component/start
                       (ke/map->EmbeddedKafka {:hostname "127.0.0.1"
                                               :port 9092
                                               :broker-id 0
                                               :log-dir log-dir
                                               :zookeeper-addr zookeeper}))
         _ (with-open [zk (kadmin/zk-client zookeeper)]
             (kadmin/create-topic zk topic
                                  {:partitions 1}))
         producer1 (kp/producer
                    {"metadata.broker.list" "127.0.0.1:9092"
                     "serializer.class" "kafka.serializer.DefaultEncoder"
                     "partitioner.class" "kafka.producer.DefaultPartitioner"})]
     (if (sequential? xs)
       (doseq [x xs]
         (kp/send-message producer1 (kp/message topic (.getBytes (pr-str x)))))
       (go-loop [itm (<! xs)]
         (if itm
           (do
             (kp/send-message (kp/producer
                               {"metadata.broker.list" "127.0.0.1:9092"
                                "serializer.class" "kafka.serializer.DefaultEncoder"
                                "partitioner.class" "kafka.producer.DefaultPartitioner"})
                              (kp/message topic (.getBytes (pr-str itm))))
             (recur (<! xs))))))
     kafka-server)))
