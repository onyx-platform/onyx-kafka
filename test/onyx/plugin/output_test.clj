(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.kafka.embedded-server :as ke]
            [com.stuartsierra.component :as component]
            [onyx.plugin.kafka]
            [onyx.kafka.utils :as util]
            [clj-kafka.admin :as kadmin]
            [clj-kafka.consumer.zk :as zk]
            [clj-kafka.core :as k]
            [onyx.api]
            [taoensso.timbre :as log :refer [fatal info]]
            [midje.sweet :refer :all]))

(def id (java.util.UUID/randomUUID))

(def zk-addr "127.0.0.1:2188")

(def env-config
  {:zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :netty
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
                            :log-dir (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))
                            :zookeeper-addr zk-addr})))

(def peer-group (onyx.api/start-peer-group peer-config))

(def topic (str "onyx-test-" (java.util.UUID/randomUUID)))

(with-open [zk (kadmin/zk-client zk-addr)]
  (kadmin/create-topic zk topic
                       {:partitions 3}))

(def workflow
  [[:in :identity]
   [:identity :write-messages]])

(defn serialize-segment [segment]
  (.getBytes (pr-str segment)))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
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
    :onyx/plugin :onyx.plugin.kafka/write-messages
    :onyx/type :output
    :onyx/medium :kafka
    :kafka/topic topic
    :kafka/zookeeper zk-addr
    :kafka/serializer-fn :onyx.plugin.output-test/serialize-segment
    :kafka/partitioner-class "kafka.producer.DefaultPartitioner"
    :onyx/batch-size 100
    :onyx/doc "Writes messages to a Kafka topic"}])

(def in-chan (chan 1000))

(>!! in-chan {:key 1
              :message {:n 0}})
(>!! in-chan {:message {:n 1}})
(>!! in-chan {:key "tarein"
              :partition 1
              :message {:n 2}})
(>!! in-chan :done)

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

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

(let [messages (util/take-until-done
                 (:zookeeper/address peer-config)
                 topic
                 (fn [v] (read-string (String. v "UTF-8"))))]
  (fact (->> messages
             (sort-by (comp :n :value))
             (map (fn [msg] 
                    (select-keys (if-not (= "tarein" (:key msg)) 
                                   (assoc msg :partition nil)
                                   msg) 
                                 [:key :value :partition]))))
        => [{:key 1
             :partition nil
             :value {:n 0}} 
            {:key nil
             :partition nil
             :value {:n 1}} 
            {:key "tarein" 
             :partition 1
             :value {:n 2}}]))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(component/stop kafka-server)
