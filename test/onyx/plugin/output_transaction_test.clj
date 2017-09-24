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

(comment (try 
          (invoke (get-value pp "transactionManager") 
                  "transitionTo" 
                  [(state-enum "READY")])
          (catch Throwable t
            (.getCause t)))

         (resume-transaction pp)


         

         (identity pp)
         (.initTransactions pp))

(comment

 (try (resume-transaction pp (short 3) (short 5)) 
     (catch Throwable t 
       (.getCause t)
       
       )
     
     ))

;(.beginTransaction pp)
;(.commitTransaction pp)
;(.beginTransaction pp)

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
               :onyx.core/results {:tree [{:leaves [{:message {:a (java.util.UUID/randomUUID)}}]}]}
               :onyx.core/task-map (:task-map (:task bundle))}
        plugin (onyx.plugin.kafka/write-messages event)
        pp (:producer plugin)
        _ (p/recover! plugin 1 nil)
        _ (println "CP" (p/checkpoint plugin))
        _ (p/write-batch plugin event nil nil)
        _ (println "TT111" (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"})))
        _ (println "TT111" (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"})))
        checkpoint (p/checkpoint plugin)
        _ (p/checkpointed! plugin 9999)
        _ (println "CP NOW" checkpoint)
        _ (println "TT11" (prepare-messages (h/take-now bootstrap-servers test-topic decompress 2000 {"isolation.level" "read_committed"})))
        _ (p/stop plugin event)
        ;plugin2 (onyx.plugin.kafka/write-messages event)
        ;pp2 (:producer plugin2)
        ;_ (p/recover! plugin2 2 checkpoint)
        ;_ (p/stop plugin2 event)
        msgs (prepare-messages (h/take-now bootstrap-servers test-topic decompress 15000))]
    (println "MSGS" msgs)
    ))
