(ns onyx.plugin.input-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx.kafka.helpers :as h]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.tasks.kafka :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api]))

(def n-partitions 4)

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :identity]
                                    [:identity :out]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn :clojure.core/identity
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                            (merge {:kafka/topic topic
                                    :kafka/group-id "onyx-consumer"
                                    :kafka/zookeeper zk-address
                                    :kafka/offset-reset :earliest
                                    :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                    :onyx/min-peers n-partitions 
                                    :onyx/max-peers n-partitions}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn write-data
  [topic zookeeper bootstrap-servers]
  (h/create-topic! zookeeper topic n-partitions 1)
  (let [producer-config {"bootstrap.servers" bootstrap-servers}
        key-serializer (h/byte-array-serializer)
        value-serializer (h/byte-array-serializer)]
    (with-open [producer1 (h/build-producer producer-config key-serializer value-serializer)]
      (with-open [producer2 (h/build-producer producer-config key-serializer value-serializer)]
        (doseq [x (range 3)] ;0 1 2
          (h/send-sync! producer1 topic nil nil (.getBytes (pr-str {:n x}))))
        (doseq [x (range 3)] ;3 4 5
          (h/send-sync! producer2 topic nil nil (.getBytes (pr-str {:n (+ 3 x)}))))))))

(deftest kafka-input-test
  (let [test-topic (str (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID)) 
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (get-core-async-channels job)]
      (with-test-env [test-env [(+ n-partitions 2) env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (write-data test-topic zk-address (:kafka-bootstrap test-config))
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))]
          (let [results (onyx.plugin.core-async/take-segments! out 10000)] 
            (println "Result" results)
            (is (= 15 (reduce + (mapv :n results)))))
          (println "Done taking segments")
          (onyx.api/kill-job peer-config job-id)))))
