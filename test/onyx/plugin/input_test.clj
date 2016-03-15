(ns onyx.plugin.input-test
  (:require [aero.core :refer [read-config]]
            [clj-kafka
             [admin :as kadmin]
             [producer :as kp]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx api 
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.kafka.embedded-server :as ke]
            [onyx.plugin kafka 
             [core-async :refer [take-segments!]]]
            [onyx.tasks.core-async :as core-async]
            [onyx.tasks.kafka :refer [kafka-input]]))

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
                                       :kafka/zookeeper zk-address
                                       :kafka/offset-reset :smallest
                                       :kafka/force-reset? true
                                       :kafka/deserializer-fn :onyx.tasks.kafka/deserialize-message-edn
                                       :onyx/min-peers 2
                                       :onyx/max-peers 2}
                                      batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(defn mock-kafka
  "Use a custom version of mock-kafka as opposed to the one in test-utils
  because we need to spawn 2 producers in order to write to each partition"
  [topic zookeeper]
  (let [kafka-server (component/start
                      (ke/map->EmbeddedKafka {:hostname "127.0.0.1"
                                              :port 9092
                                              :broker-id 0
                                              :log-dir (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))
                                              :zookeeper-addr zookeeper}))
        _ (with-open [zk (kadmin/zk-client zookeeper)]
            (kadmin/create-topic zk topic
                                 {:partitions 2}))
        producer1 (kp/producer
                   {"metadata.broker.list" "127.0.0.1:9092"
                    "serializer.class" "kafka.serializer.DefaultEncoder"
                    "partitioner.class" "kafka.producer.DefaultPartitioner"})
        producer2 (kp/producer
                   {"metadata.broker.list" "127.0.0.1:9092"
                    "serializer.class" "kafka.serializer.DefaultEncoder"
                    "partitioner.class" "kafka.producer.DefaultPartitioner"})]

    (do (doseq [x (range 3)] ;0 1 2
          (kp/send-message producer1 (kp/message topic (.getBytes (pr-str {:n x})))))
        (doseq [x (range 3)] ;3 4 5
          (kp/send-message producer2 (kp/message topic (.getBytes (pr-str {:n (+ 3 x)})))))
        kafka-server)))

(deftest kafka-input-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 10 1000)
        {:keys [out read-messages]} (core-async/get-core-async-channels job)
        mock (atom {})]
    (try
      (with-test-env [test-env [4 env-config peer-config]]
        (onyx.test-helper/validate-enough-peers! test-env job)
        (reset! mock (mock-kafka test-topic zk-address))
        (onyx.api/submit-job peer-config job)
        (is (= 15
               (reduce + (mapv :n (onyx.plugin.core-async/take-segments! out 5000))))))
      (finally (swap! mock component/stop)))))
