(ns onyx.plugin.output-test
  (:require [aero.core :refer [read-config]]
            [clj-kafka
             [admin :as kadmin]
             [producer :as kp]]
            [clojure.core.async :refer [<!! go pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.kafka
             [embedded-server :as ke]
             [utils :refer [take-until-done]]]
            [onyx.plugin kafka
             [core-async :refer [take-segments! get-core-async-channels]]
             [test-utils :as test-utils]]
            [onyx.tasks
             [kafka :refer [producer]]
             [core-async :as core-async]]))

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow   [[:in :identity]
                                      [:identity :write-messages]]
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
        (add-task (core-async/input :in batch-settings))
        (add-task (producer :write-messages
                                (merge {:kafka/topic topic
                                        :kafka/zookeeper zk-address
                                        :kafka/serializer-fn :onyx.tasks.kafka/serialize-message-edn
                                        :kafka/request-size 307200}
                                       batch-settings))))))

(deftest kafka-output-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [in]} (get-core-async-channels job)
        mock (atom {})
        test-data [{:key 1 :message {:n 0}} {:message {:n 1}}
                   {:key "tarein" :message {:n 2}} :done]]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (test-utils/mock-kafka test-topic zk-address []))
        (pipe (spool test-data) in) ;; Pipe data from test-data to the in channel
        (onyx.api/submit-job peer-config job)
        (is (= (->> (take-until-done zk-address test-topic (fn [v] (read-string (String. v "UTF-8"))))
                    (sort-by (comp :n :value))
                    (mapv (fn [msg]
                            (select-keys msg [:key :value :partition]))))
               [{:key 1 :value {:n 0} :partition 0}
                {:key nil :value {:n 1} :partition 0}
                {:key "tarein" :value {:n 2} :partition 0}])))
      (finally (swap! mock component/stop)))))
