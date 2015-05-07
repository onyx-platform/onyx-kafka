(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.kafka]
            [clj-kafka.consumer.zk :as zk]
            [clj-kafka.core :as k]
            [onyx.api]
            [midje.sweet :refer :all]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :netty
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/peer-ports [40199]
   :onyx.messaging/bind-addr "localhost"
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def topic (str "onyx-test-" (java.util.UUID/randomUUID)))

(def workflow
  [[:in :identity]
   [:identity :write-messages]])

(def catalog
  [{:onyx/name :in
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size 100
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size 100}

   {:onyx/name :write-messages
    :onyx/ident :kafka/write-messages
    :onyx/type :output
    :onyx/medium :kafka
    :kafka/topic topic
    :kafka/brokers "127.0.0.1:9092"
    :kafka/serializer-class "kafka.serializer.DefaultEncoder"
    :kafka/partitioner-class "kafka.producer.DefaultPartitioner"
    :onyx/batch-size 100
    :onyx/doc "Writes messages to a Kafka topic"}])

(def in-chan (chan 1000))

(>!! in-chan {:n 0})
(>!! in-chan {:n 1})
(>!! in-chan {:n 2})
(>!! in-chan :done)

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task inject-in-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.output-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :write-messages
    :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def config
  {"zookeeper.connect" "127.0.0.1:2181"
   "group.id" "onyx-test-consumer"
   "auto.offset.reset" "smallest"
   "auto.commit.enable" "false"})

(k/with-resource [c (zk/consumer config)]
  zk/shutdown
  (fact (take 4 (map (fn [v] (read-string (String. (:value v) "UTF-8"))) (zk/messages c topic)))
        => [{:n 0} {:n 1} {:n 2} :done]))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

