(ns onyx.plugin.output-bench-test
  (:require [clojure.core.async :refer [<!! go pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.cluster :as k-cluster]
            [aero.core :refer [read-config]]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.embedded-server :as ke]
            [taoensso.nippy :as nip]
            [onyx.kafka.utils :refer [take-now take-until-done]]
            [onyx.tasks.kafka :refer [producer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api]
            [taoensso.timbre :as log]))

(def compress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn compress [x]
  (nip/freeze x compress-opts))

(def decompress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn decompress [x]
  (nip/thaw x decompress-opts))

(def n-messages-total 2000000)

(defn wrap-message [segment]
  {:message segment})

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size
                        :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow  [[:in :write-messages]]
                         :catalog []
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings 10000000))
        (add-task (producer :write-messages
                            (merge {:kafka/topic topic
                                    :kafka/zookeeper zk-address
                                    :kafka/serializer-fn ::compress
                                    :onyx/fn ::wrap-message
                                    :kafka/request-size 307200}
                                   batch-settings))))))

(deftest ^:benchmark kafka-output-benchmark-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :bench})
        tenancy-id (java.util.UUID/randomUUID)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        peer-group (onyx.api/start-peer-group peer-config)
        v-peers (onyx.api/start-peers 2 peer-group)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 500 5)
        {:keys [in]} (get-core-async-channels job)
        mock (atom {})
        test-data (conj (mapv (fn [v] {:n v}) (range n-messages-total)) :done)]
    (try
     (println "Spooling test data")
     (pipe (spool test-data) in) ;; Pipe data from test-data to the in channel
     (println "test data out")
     (let [start-time (System/currentTimeMillis)] 
       (->> (onyx.api/submit-job peer-config job)
            :job-id
            (onyx.test-helper/feedback-exception! peer-config))
       (testing "routing to default topic"
         (log/info "Waiting on messages in" test-topic)
         (let [run-time (- (System/currentTimeMillis) start-time)
               msgs (take-until-done zk-address test-topic decompress {:timeout 1800000})]
           (is (= (butlast (map :n test-data)) (map :n (sort-by :n (map :value msgs)))))
           (println (float (* 1000 (/ n-messages-total run-time))) "messages per second. Processed" n-messages-total "messages in" run-time "ms."))))
     (finally
      (log/info "Stopping mock Kafka...")
      (doseq [p v-peers]
        (onyx.api/shutdown-peer p))
      (onyx.api/shutdown-peer-group peer-group)))))
