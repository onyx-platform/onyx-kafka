(ns onyx.plugin.input-multipartition-scheduling-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.tasks.kafka :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.kafka.helpers :as h]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.kafka]
            [onyx.api]))

(def n-partitions 4)

(defn build-job [zk-address topic batch-size batch-timeout n-input-peers]
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
                                    :onyx/n-peers n-partitions}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn write-data
  [topic zookeeper kafka-bootstrap n-segments-each]
  (h/create-topic! zookeeper topic n-partitions 1)
  (let [producer-config {"bootstrap.servers" kafka-bootstrap}
        key-serializer (h/byte-array-serializer)
        value-serializer (h/byte-array-serializer)]
    (with-open [producer1 (h/build-producer producer-config key-serializer value-serializer)]
      (with-open [producer2 (h/build-producer producer-config key-serializer value-serializer)]
        (doseq [x (range n-segments-each)] ;0 1 2
          (h/send-sync! producer1 topic nil nil (.getBytes (pr-str {:n x}))))
        (doseq [x (range n-segments-each)] ;3 4 5
          (h/send-sync! producer2 topic nil nil (.getBytes (pr-str {:n (+ n-segments-each x)}))))))))

(deftest kafka-multipartition-scheduling-test
  (let [test-topic (str (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        kafka-bootstrap (:kafka-bootstrap test-config)
        tenancy-id (str (java.util.UUID/randomUUID)) 
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        ;; randomize number of peers so that we can check if number of partitions
        ;; is correctly assigned
        n-peers (inc (rand-int n-partitions))
        _ (println "Random number of npeers" n-peers)
        job (build-job zk-address test-topic 10 1000 n-peers)
        {:keys [out read-messages]} (get-core-async-channels job)
        n-segments-each 200]
      (with-test-env [test-env [(+ n-partitions 2) env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (write-data test-topic zk-address kafka-bootstrap n-segments-each)
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))]
          (println "Taking segments")
          ;(onyx.test-helper/feedback-exception! peer-config job-id)
          (let [results (onyx.plugin.core-async/take-segments! out 10000)] 
            (is (= (range (* 2 n-segments-each)) (sort (mapv :n results)))))
          (println "Done taking segments")
          (onyx.api/kill-job peer-config job-id)))))
