(ns onyx.plugin.input-static-partition-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.kafka.tasks :refer [kafka-input]]
            [onyx.plugin
             [test-utils :as test-utils]
             [core-async :refer [take-segments!]]
             [core-async-tasks :as core-async]
             [test-utils :as test-utils]
             [kafka]]))

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
        (add-task (kafka-input :read-messages
                               (merge {:kafka/topic topic
                                       :kafka/group-id "onyx-consumer"
                                       :kafka/zookeeper "127.0.0.1:2181"
                                       :kafka/offset-reset :smallest
                                       :kafka/force-reset? true
                                       :kafka/deserializer-fn :onyx.kafka.tasks/deserialize-message-edn
                                       :onyx/min-peers 1
                                       :onyx/max-peers 1}
                                      batch-settings)))
        (add-task (core-async/output-task :out batch-settings)))))

(deftest kafka-static-partition-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        zk-address "127.0.0.1:2181"
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (core-async/get-core-async-channels job)
        test-data [{:n 1} {:n 2} {:n 3} :done]
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (test-utils/mock-kafka test-topic zk-address test-data))
        (onyx.api/submit-job peer-config job)
        (is (= (onyx.plugin.core-async/take-segments! out)
               test-data)))
      (finally (swap! mock component/stop)))))
