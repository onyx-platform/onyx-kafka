(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan <!! >!! close!]]
            [clojure.data.fressian :as fressian]
            [midje.sweet :refer :all]
            [clj-kafka.consumer.zk :as zk]
            [clj-kafka.core :as k]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async]
            [onyx.plugin.kafka]
            [onyx.api]))

(def topic (str "onyx-test-" (java.util.UUID/randomUUID)))

(def workflow {:in {:identity :write-messages}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1}

   {:onyx/name :write-messages
    :onyx/ident :kafka/write-messages
    :onyx/type :output
    :onyx/medium :kafka
    :onyx/consumption :concurrent
    :kafka/topic topic
    :kafka/brokers "127.0.0.1:9092"
    :kafka/serializer-class "kafka.serializer.DefaultEncoder"
    :kafka/partitioner-class "kafka.producer.DefaultPartitioner"
    :onyx/batch-size 1
    :onyx/doc "Writes messages to a Kafka topic"}])

(def in-chan (chan 1000))

(defmethod l-ext/inject-lifecycle-resources :in
  [_ _] {:core-async/in-chan in-chan})

(>!! in-chan {:n 0})
(>!! in-chan {:n 1})
(>!! in-chan {:n 2})
(>!! in-chan :done)

(close! in-chan)

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def config
  {"zookeeper.connect" "127.0.0.1:2181"
   "group.id" "onyx-test-consumer"
   "auto.offset.reset" "smallest"
   "auto.commit.enable" "false"})

(k/with-resource [c (zk/consumer config)]
  zk/shutdown
  (fact (take 4 (map fressian/read (map :value (zk/messages c topic))))
        => [{:n 0} {:n 1} {:n 2} :done]))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

