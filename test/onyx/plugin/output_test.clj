(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [<!! go pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.cluster :as k-cluster]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.embedded-server :as ke]
            [onyx.kafka.utils :refer [take-now take-until-done]]
            [onyx.tasks.kafka :refer [producer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api]
            [taoensso.timbre :as log]))

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size
                        :onyx/batch-timeout batch-timeout}
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

(defn- decompress
  [v]
  (when v
    (read-string (String. v "UTF-8"))))

(defn- prepare-messages
  [coll]
  (log/infof "Preparing %d messages..." (count coll))
  (->> coll
       (sort-by (comp :n :value))
       (map #(select-keys % [:key :partition :topic :value]))))

(deftest kafka-output-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        other-test-topic (str "onyx-test-other-" (java.util.UUID/randomUUID))
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [in]} (get-core-async-channels job)
        mock (atom {})
        test-data [{:key 1 :message {:n 0}}
                   {:message {:n 1}}
                   {:key "tarein" :message {:n 2}}
                   {:message {:n 3} :topic other-test-topic}
                   :done]]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (test-utils/mock-kafka test-topic zk-address [] (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID)) (:embedded-kafka? test-config)))
        (test-utils/create-topic zk-address other-test-topic)
        (pipe (spool test-data) in) ;; Pipe data from test-data to the in channel
        (->> (onyx.api/submit-job peer-config job)
             :job-id
             (onyx.test-helper/feedback-exception! peer-config))
        (testing "routing to default topic"
          (log/info "Waiting on messages in" test-topic)
          (let [msgs (prepare-messages
                      (take-until-done zk-address test-topic decompress))]
            (is (= [test-topic] (->> msgs (map :topic) distinct)))
            (is (= [{:key 1 :value {:n 0} :partition 0}
                    {:key nil :value {:n 1} :partition 0}
                    {:key "tarein" :value {:n 2} :partition 0}]
                   (map #(dissoc % :topic) msgs)))))
        (testing "overriding the topic"
          (log/info "Waiting on messages in" other-test-topic)
          (is (= [{:key nil :value {:n 3} :partition 0 :topic other-test-topic}]
                 (prepare-messages
                  (take-now zk-address other-test-topic decompress))))))
      (finally
        (log/info "Stopping mock Kafka...")
        (swap! mock component/stop)))))
