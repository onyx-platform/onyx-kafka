(ns onyx.plugin.input-broker-reboot-test
  (:require [aero.core :refer [read-config]]
            [clojure.core.async :refer [>!! chan]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.kafka.tasks :refer [kafka-input]]
            [onyx.plugin
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
                         :lifecycles [{:lifecycle/task :read-messages
                                       :lifecycle/calls ::restartable-reader}]
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (kafka-input :read-messages
                               (merge {:kafka/topic topic
                                       :kafka/group-id "onyx-consumer"
                                       :kafka/zookeeper zk-address
                                       :kafka/offset-reset :smallest
                                       :kafka/force-reset? false
                                       :kafka/deserializer-fn :onyx.kafka.tasks/deserialize-message-edn
                                       :onyx/max-peers 1
                                       :onyx/batch-size 2}
                                      batch-settings)))
        (add-task (core-async/output-task :out batch-settings)))))

(def restartable-reader
  {:lifecycle/handle-exception (constantly :restart)})

(deftest kafka-broker-reboot-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 2 1000)
        {:keys [out read-messages]} (core-async/get-core-async-channels job)
        test-data1 [{:n 1}]
        test-data2 [{:n 2} {:n 3} {:n 4} {:n 5} {:n 6} :done]
        input-chan (chan 10)
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (doseq [x test-data1] (>!! input-chan x))
        (reset! mock (test-utils/mock-kafka test-topic zk-address input-chan "/tmp/embedded-kafka2"))
        (onyx.api/submit-job peer-config job)
        (Thread/sleep 5000)
        (swap! mock component/stop)
        (swap! mock component/start)
        (Thread/sleep 5000)
        (doseq [x test-data2] (>!! input-chan x))
        (is (= (set (into test-data1 test-data2))
               (set (onyx.plugin.core-async/take-segments! out)))))
      (finally (swap! mock component/stop)))))
