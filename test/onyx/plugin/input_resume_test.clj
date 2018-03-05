(ns onyx.plugin.input-resume-test
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
  (let [batch-settings {:onyx/batch-size batch-size
                        :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :identity]
                                    [:identity :out]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn :clojure.core/identity
                                           :onyx/n-peers 1
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles [{:lifecycle/task :read-messages
                                       :lifecycle/calls ::read-crash}]
                         :windows [{:window/id :collect-segments
                                    :window/task :identity
                                    :window/type :global
                                    :window/aggregation :onyx.windowing.aggregation/conj}]
                         :triggers [{:trigger/window-id :collect-segments
                                     :trigger/fire-all-extents? true
                                     :trigger/id :collect-trigger
                                     :trigger/on :onyx.triggers/segment
                                     :trigger/threshold [1 :elements]
                                     :trigger/sync ::update-atom!}]
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                               (merge {:kafka/topic topic
                                       :kafka/group-id "onyx-consumer"
                                       :kafka/zookeeper zk-address
                                       :kafka/offset-reset :earliest
                                       :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                       :onyx/max-peers 1
                                       :onyx/batch-size 1}
                                      batch-settings)))
        (add-task (core-async/output :out batch-settings 100000)))))

(def batch-num (atom 0))

(def test-state (atom #{}))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (swap! test-state into extent-state))

(def read-crash
  {:lifecycle/before-batch
   (fn [event lifecycle]
     (when (= (swap! batch-num inc) 4000)
       (throw (ex-info "Restartable" {:restartable? true}))))
   :lifecycle/handle-exception (constantly :restart)})

(deftest kafka-resume-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config
                           :onyx/tenancy-id tenancy-id
                           ;; should be much lower to get some checkpointing in
                           :onyx.peer/coordinator-barrier-period-ms 1)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 2 1000)
        test-data (conj (mapv (fn [v] {:n v}) (range 10000)) :done)]
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (test-utils/write-data test-topic zk-address (:kafka-bootstrap test-config) test-data)
        (->> job
             (onyx.api/submit-job peer-config)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (Thread/sleep 1000)
        (let [{:keys [out]} (get-core-async-channels job)]
          (is (= (set (butlast test-data)) (set @test-state)))))))
