(ns onyx.plugin.input-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.kafka]
            [clj-kafka.producer :as kp]
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
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/peer-ports [40199]
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def topic (str "onyx-test-" (java.util.UUID/randomUUID)))

(def producer
  (kp/producer
   {"metadata.broker.list" "127.0.0.1:9092"
    "serializer.class" "kafka.serializer.DefaultEncoder"
    "partitioner.class" "kafka.producer.DefaultPartitioner"}))

(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 1}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 2}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 3}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str :done))))

(def workflow
  [[:read-messages :identity]
   [:identity :out]])

(def catalog
  [{:onyx/name :read-messages
    :onyx/ident :kafka/read-messages
    :onyx/type :input
    :onyx/medium :kafka
    :kafka/topic topic
    :kafka/zookeeper "127.0.0.1:2181"
    :kafka/group-id "onyx-consumer"
    :kafka/offset-reset "smallest"
    :onyx/max-peers 1
    :onyx/batch-size 100
    :onyx/doc "Reads messages from a Kafka topic"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size 100}

   {:onyx/name :out
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 100
    :onyx/doc "Writes segments to a core.async channel"}])

(def out-chan (chan 100))

(defmethod l-ext/inject-lifecycle-resources :out
  [_ _] {:core.async/chan out-chan})

(def v-peers (onyx.api/start-peers 3 peer-group))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/balanced})

(def results (doall (map (fn [_] (read-string (String. (<!! out-chan) "UTF-8"))) (range 4))))

(fact results => [{:n 1} {:n 2} {:n 3} :done])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

