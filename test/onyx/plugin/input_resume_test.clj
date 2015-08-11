(ns onyx.plugin.input-resume-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.kafka]
            [com.stuartsierra.component :as component]
            [clj-kafka.producer :as kp]
            [onyx.kafka.embedded-server :as ke]
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
   :onyx/id id})

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
(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 4}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 5}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str {:n 6}))))
(kp/send-message producer (kp/message topic (.getBytes (pr-str :done))))

(defn deserialize-message [bytes]
  (read-string (String. bytes "UTF-8")))

(def workflow
  [[:read-messages :identity]
   [:identity :out]])

(defn restartable? [e] 
  true)

(def catalog
  [{:onyx/name :read-messages
    :onyx/plugin :onyx.plugin.kafka/read-messages
    :onyx/type :input
    :onyx/medium :kafka
    :kafka/topic topic
    :kafka/partition "0"
    :kafka/group-id "onyx-consumer"
    :kafka/fetch-size 307200
    :kafka/chan-capacity 1000
    :kafka/zookeeper "127.0.0.1:2188"
    :kafka/offset-reset :smallest
    :kafka/force-reset? false
    :kafka/empty-read-back-off 500
    :kafka/commit-interval 500
    :kafka/deserializer-fn :onyx.plugin.input-resume-test/deserialize-message
    :onyx/restart-pred-fn :onyx.plugin.input-resume-test/restartable?
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
    :onyx/batch-size 2
    :onyx/doc "Writes segments to a core.async channel"}])

(def out-chan (chan 100))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def batch-num (atom 0))

(def read-crash
  {:lifecycle/before-batch 
   (fn [event lifecycle]
     ; give the peer a bit of time to write the chunks out and ack the batches,
     ; since we want to ensure that the batches aren't re-read on restart
     (Thread/sleep 7000)
     (when (= (swap! batch-num inc) 2)
       (throw (ex-info "Restartable" {:restartable? true}))))})

(def lifecycles
  [{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
   {:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.input-resume-test/read-crash}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.input-resume-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :lifecycles lifecycles
  :task-scheduler :onyx.task-scheduler/balanced})

(def results (doall (map (fn [_] (<!! out-chan)) (range 7))))

(fact results => [{:n 1} {:n 2} {:n 3} {:n 4} {:n 5} {:n 6} :done])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(component/stop kafka-server)
