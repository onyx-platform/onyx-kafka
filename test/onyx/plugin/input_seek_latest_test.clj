(ns onyx.plugin.input-seek-latest-test
  (:require [clojure.core.async :refer [<!! go pipe]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.cluster :as k-cluster]
            [franzy.admin.topics :as k-topics]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.protocols :refer [send-sync!]]
            [aero.core :refer [read-config]]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.utils]
            [onyx.tasks.kafka :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api])
  (:import [franzy.clients.producer.types ProducerRecord]))

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
                                    :kafka/offset-reset :latest
                                    :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                    :onyx/min-peers 2
                                    :onyx/max-peers 2}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn write-messages
  "Use a custom version of mock-kafka as opposed to the one in test-utils
  because we need to spawn 2 producers in order to write to each partition"
  [topic zookeeper]
  (let [producer-config {:bootstrap.servers ["127.0.0.1:9092"]}
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)]
    (with-open [producer1 (producer/make-producer producer-config key-serializer value-serializer)]
      (with-open [producer2 (producer/make-producer producer-config key-serializer value-serializer)]
        (doseq [x (range 3)] ;0 1 2
          (send-sync! producer1 (ProducerRecord. topic nil nil (.getBytes (pr-str {:n x})))))
        (doseq [x (range 3)] ;3 4 5
          (send-sync! producer2 (ProducerRecord. topic nil nil (.getBytes (pr-str {:n (+ 3 x)})))))))))

(deftest kafka-input-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (get-core-async-channels job)]
    (with-test-env [test-env [4 env-config peer-config]]
      (onyx.test-helper/validate-enough-peers! test-env job)
      (test-utils/create-topic zk-address test-topic 2)
      (write-messages test-topic zk-address)
      (let [job-id (:job-id (onyx.api/submit-job peer-config job))]
        (Thread/sleep 2000)
        (write-messages test-topic zk-address)
        (is (= 15 (reduce + (mapv :n (onyx.plugin.core-async/take-segments! out 10000)))))
        (onyx.api/kill-job peer-config job-id)
        (Thread/sleep 10000)))))
