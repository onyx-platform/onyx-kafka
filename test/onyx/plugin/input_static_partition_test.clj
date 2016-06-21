(ns onyx.plugin.input-static-partition-test
  (:require [aero.core :refer [read-config]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin kafka
             [core-async :refer [take-segments! get-core-async-channels]]
             [test-utils :as test-utils]]
            [onyx.tasks
             [kafka :refer [consumer]]
             [core-async :as core-async]]))

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
                                       :kafka/offset-reset :smallest
                                       :kafka/force-reset? true
                                       :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                       :onyx/min-peers 1
                                       :onyx/max-peers 1}
                                      batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(deftest kafka-static-partition-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        zk-address (get-in peer-config [:zookeeper/address])
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (get-core-async-channels job)
        test-data [{:n 1} {:n 2} {:n 3} :done]
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (test-utils/mock-kafka test-topic zk-address test-data))
        (->> (onyx.api/submit-job peer-config job)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (is (= test-data
               (onyx.plugin.core-async/take-segments! out))))
      (finally (swap! mock component/stop)))))
