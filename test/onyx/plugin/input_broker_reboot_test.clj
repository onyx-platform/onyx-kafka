(ns onyx.plugin.input-broker-reboot-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.kafka]
            [com.stuartsierra.component :as component]
            [clj-kafka.producer :as kp]
            [onyx.kafka.embedded-server :as ke]
            [onyx.api]
            [midje.sweet :refer :all]))

(comment (def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40199
   :onyx.messaging/bind-addr "localhost"
   :onyx/tenancy-id id})

(def env (onyx.api/start-env env-config))

(def kafka-server
  (component/start 
    (ke/map->EmbeddedKafka {:hostname "127.0.0.1" 
                            :port 9092
                            :broker-id 0
                            :log-dir "/tmp/embedded-kafka2"
                            :zookeeper-addr "127.0.0.1:2188"})))

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

(defn deserialize-message [bytes]
  (read-string (String. bytes "UTF-8")))

(def workflow
  [[:read-messages :identity]
   [:identity :out]])

(def catalog
  [{:onyx/name :read-messages
    :onyx/plugin :onyx.plugin.kafka/read-messages
    :onyx/type :input
    :onyx/medium :kafka
    :kafka/topic topic
    :kafka/group-id "onyx-consumer"
    :kafka/fetch-size 307200
    :kafka/chan-capacity 1000
    :kafka/zookeeper "127.0.0.1:2188"
    :kafka/offset-reset :smallest
    :kafka/force-reset? false
    :kafka/empty-read-back-off 500
    :kafka/commit-interval 500
    :kafka/deserializer-fn ::deserialize-message
    :onyx/max-peers 1
    :onyx/batch-size 2
    :onyx/doc "Reads messages from a Kafka topic"}

   {:onyx/name :identity
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/batch-size 2}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size 2
    :onyx/doc "Writes segments to a core.async channel"}])

(def out-chan (chan 100))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def batch-num (atom 0))

(def restartable-reader
  {:lifecycle/handle-exception (constantly :restart)})

(def lifecycles
  [{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
   {:lifecycle/task :read-messages
    :lifecycle/calls ::restartable-reader}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(Thread/sleep 20000)

(def stopped-server (component/stop kafka-server))

(Thread/sleep 6000)

(def started-again (component/start stopped-server))

(def producer2
  (kp/producer
   {"metadata.broker.list" "127.0.0.1:9092"
    "serializer.class" "kafka.serializer.DefaultEncoder"
    "partitioner.class" "kafka.producer.DefaultPartitioner"}))

(kp/send-message producer2 (kp/message topic (.getBytes (pr-str {:n 4}))))
(kp/send-message producer2 (kp/message topic (.getBytes (pr-str {:n 5}))))
(kp/send-message producer2 (kp/message topic (.getBytes (pr-str {:n 6}))))
(kp/send-message producer2 (kp/message topic (.getBytes (pr-str :done))))

(def results (take-segments! out-chan))

(fact (set results) => #{{:n 1} {:n 2} {:n 3} {:n 4} {:n 5} {:n 6} :done})

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(component/stop kafka-server))
