(ns onyx.plugin.input-start-offset-test
  (:require [clojure.core.async :refer [<!! go pipe]]
            [clojure.core.async.lab :refer [spool]]
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
            [onyx.kafka.embedded-server :as ke]
            [onyx.kafka.utils :refer [take-until-done]]
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
                                    :kafka/offset-reset :earliest
                                    :kafka/force-reset? false
                                    :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                    :kafka/start-offsets {0 1}
                                    :onyx/min-peers 1
                                    :onyx/max-peers 1}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn mock-kafka
  "Use a custom version of mock-kafka as opposed to the one in test-utils
  because we need to spawn 2 producers in order to write to each partition"
  [topic zookeeper embedded-kafka?]
  (let [kafka-server (component/start
                      (ke/embedded-kafka {:advertised.host.name "127.0.0.1"
                                          :port 9092
                                          :server? embedded-kafka?
                                          :broker.id 1
                                          :log.dir (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))
                                          :zookeeper.connect zookeeper
                                          :controlled.shutdown.enable false}))

        zk-utils (k-admin/make-zk-utils {:servers [zookeeper]} false)
        _ (k-topics/create-topic! zk-utils topic 1)

        producer-config {:bootstrap.servers ["127.0.0.1:9092"]}
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)]
    (with-open [producer1 (producer/make-producer producer-config key-serializer value-serializer)]
      (doseq [x (range 5)] ;0 1 2
        (send-sync! producer1 (ProducerRecord. topic nil nil (.getBytes (pr-str {:n x}))))))
    kafka-server))

(deftest kafka-input-start-offset-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        _ (println "Using topic" test-topic)
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (get-core-async-channels job)
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (mock-kafka test-topic zk-address (:embedded-kafka? test-config)))
        (onyx.api/submit-job peer-config job)
        (is (= [{:n 1} {:n 2} {:n 3} {:n 4}] (onyx.plugin.core-async/take-segments! out 10000))))
      (finally (swap! mock component/stop)))))
