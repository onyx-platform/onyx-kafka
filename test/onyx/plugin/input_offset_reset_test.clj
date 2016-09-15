(ns onyx.plugin.input-offset-reset-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async.lab :refer [spool]]
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
                         :lifecycles [{:lifecycle/task :read-messages
                                       :lifecycle/calls ::read-crash}]
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
                                       :kafka/commit-interval 500
                                       :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                       :onyx/pending-timeout 20000
                                       :onyx/max-peers 1
                                       :onyx/batch-size 2}
                                      batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(def batch-num (atom 0))

(def read-crash
  {:lifecycle/before-batch
   (fn [event lifecycle]
     ; give the peer a bit of time to write the chunks out and ack the batches,
     ; since we want to ensure that the batches aren't re-read on restart for ease of testing
     (Thread/sleep 5000)
     (when (= (swap! batch-num inc) 2)
       (throw (ex-info "Restartable" {:restartable? true}))))
   :lifecycle/handle-exception (constantly :restart)})

(deftest kafka-offset-reset-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)

        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 2 1000)
        {:keys [out read-messages]} (get-core-async-channels job)
        test-data [{:n 1} {:n 2} {:n 3} {:n 4} {:n 5} {:n 6} :done]
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (test-utils/mock-kafka test-topic zk-address test-data))
        (onyx.api/submit-job peer-config job)
        (is (= (into [{:n 1} {:n 2}] test-data)
               (onyx.plugin.core-async/take-segments! out))))
      (finally (swap! mock component/stop)))))
