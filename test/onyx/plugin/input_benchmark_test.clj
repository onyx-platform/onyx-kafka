(ns onyx.plugin.input-benchmark-test
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
            [franzy.clients.producer.protocols :refer [send-async!]]
            [aero.core :refer [read-config]]
            [taoensso.nippy :as nip]
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

(def compress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn compress [x]
  (nip/freeze x compress-opts))

(def decompress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn decompress [x]
  (nip/thaw x decompress-opts))

(def messages-per-partition 600000)
(def n-partitions 3)

(defn print-message [segment]
  (println "Read " segment)
  segment)

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :out]]
                         :catalog []
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
                                    :kafka/deserializer-fn ::decompress
                                    :onyx/fn ::print-message
                                    ;:kafka/fetch-size 3072000
                                    ;:kafka/request-size 3072000
                                    :kafka/chan-capacity 100000
                                    :kafka/poll-timeout-ms 500
                                    :onyx/max-pending 100000
                                    :onyx/min-peers n-partitions
                                    :onyx/max-peers n-partitions}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings 100000000 #_(inc (* n-partitions messages-per-partition)))))))

(defn mock-kafka
  "Use a custom version of mock-kafka as opposed to the one in test-utils
  because we need to spawn 2 producers in order to write to each partition"
  [topic zookeeper]
  (let [kafka-server (component/start
                      (ke/embedded-kafka {:advertised.host.name "127.0.0.1"
                                          :port 9092
                                          :broker.id 0
                                          :log.dir (str "/tmp/embedded-kafka" (java.util.UUID/randomUUID))
                                          :zookeeper.connect zookeeper
                                          :controlled.shutdown.enable false}))

        zk-utils (k-admin/make-zk-utils {:servers [zookeeper]} false)
        _ (k-topics/create-topic! zk-utils topic n-partitions)

        producer-config {:bootstrap.servers ["127.0.0.1:9092"]}
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)

        producer1 (producer/make-producer producer-config key-serializer value-serializer)]

    (time 
     (doseq [p (range n-partitions)]
       (mapv deref 
             (doall (map (fn [x]
                           (send-async! producer1 (ProducerRecord. topic p nil (compress {:n x})))) 
                         (range messages-per-partition))))))
    (println "Successfully wrote messages")
    kafka-server))

(deftest kafka-input-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :test})
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config 
                           :onyx/tenancy-id tenancy-id
                           :onyx.messaging/allow-short-circuit? true)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        n-peers (+ 2 n-partitions)
        v-peers (onyx.api/start-peers n-peers peer-group)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 1000 50)
        {:keys [out read-messages]} (get-core-async-channels job)
        mock (atom {})]
    (try
     (reset! mock (mock-kafka test-topic zk-address))
     (let [job-id (:job-id (onyx.api/submit-job peer-config job))
           start-time (System/currentTimeMillis)
           read-nothing-timeout 100000]
       (is (= (* n-partitions messages-per-partition) (count (onyx.plugin.core-async/take-segments! out read-nothing-timeout)))) 
       (let [run-time (- (System/currentTimeMillis) start-time read-nothing-timeout)
             n-messages-total (* n-partitions messages-per-partition)]
         (println (float (* 1000 (/ n-messages-total run-time))) "messages per second. Processed" n-messages-total "messages in" run-time "ms."))
       (onyx.api/kill-job peer-config job-id))
     (finally 
      (doseq [p v-peers]
        (onyx.api/shutdown-peer p))
      (onyx.api/shutdown-peer-group peer-group)
      (onyx.api/shutdown-env env)

      (swap! mock component/stop)))))
