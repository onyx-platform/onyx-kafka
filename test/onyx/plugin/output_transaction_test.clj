(ns onyx.plugin.output-transaction-test
  (:require [clojure.core.async :refer [<!! go pipe close! >!!]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.helpers :as h]
            [onyx.tasks.kafka :refer [producer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.plugin.protocols :as p]
            [onyx.api]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.clients.producer.internals.TransactionManager$State.READY])
  
  )

(defn- decompress
  [v]
  (when v
    (read-string (String. v "UTF-8"))))

(defn- prepare-messages
  [coll]
  (log/infof "Preparing %d messages..." (count coll))
  (->> coll
       (sort-by (comp :n :value))
       (map #(select-keys % [:key :partition :topic :value]))))

(deftest kafka-output-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID)) 
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        bootstrap-servers (:kafka-bootstrap test-config)
        _ (println "ZK" zk-address)
        bundle (producer :write-messages
                         {:kafka/topic test-topic
                          :kafka/zookeeper zk-address
                          :kafka/serializer-fn :onyx.tasks.kafka/serialize-message-edn
                          :kafka/key-serializer-fn :onyx.tasks.kafka/serialize-message-edn
                          :kafka/request-size 307200})
        event {:onyx.core/slot-id 0
               :onyx.core/log-prefix ""
               :onyx.core/job-id (java.util.UUID/randomUUID)
               :onyx.core/task-map (:task-map (:task bundle))}
        plugin (onyx.plugin.kafka/write-messages event)
        pp (:producer plugin)
        _ (p/recover! plugin 0 nil)
        _ (p/synced? plugin 0)
        _ (p/write-batch plugin 
                         (assoc event :onyx.core/results {:tree [{:leaves [{:message {:a 1}}]}]}) 
                         nil 
                         nil)
        _ (is (empty? (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"}))))
        checkpoint (p/checkpoint plugin)
        _ (p/synced? plugin 1)
        checkpoint2 (p/checkpoint plugin)
        _ (p/write-batch plugin 
                         (assoc event :onyx.core/results {:tree [{:leaves [{:message {:a 2}}]}]}) 
                         nil 
                         nil)
        _ (is (empty? (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"}))))
        _ (p/synced? plugin 2)
        _ (p/checkpointed! plugin 1)
        _ (p/checkpointed! plugin 2)
        _ (is (= [{:a 1} {:a 2}] (map :value (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"})))))
        _ (p/stop plugin event)]))
