(ns onyx.plugin.input-benchmark-test
  (:require [clojure.core.async :refer [<!! go pipe timeout]]
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


;;;;;;;;;;;;;;;;;;;;;;;
;;;
; Note, this test requires a non embedded Kafka and ZooKeeper. ZooKeeper must be running on 2181 (not 2188 as Onyx generally uses) 
; Kafka must run on port 127.0.0.1:9092
;;;
;;;;;;;;;;;;;;;;;;;;;;;


(def compress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn compress [x]
  (nip/freeze x compress-opts))

(def decompress-opts {:v1-compatibility? false :compressor nil :encryptor nil :password nil})

(defn decompress [x]
  (nip/thaw x decompress-opts))

(def messages-per-partition 2000000)
(def n-partitions 1)

(defn print-message [segment]
  segment
  ; (if (zero? (mod (:n segment) 10))
  ;   segment
  ;   [])
  )

(defn ignore-some-messages [segment]
  (even? (:n segment)))

(defn flow-on? [event old segment all-new]
  (even? (:n segment)))

(defn build-job [zk-address topic batch-size batch-timeout]
  (let [base-job (merge {:workflow [[:read-messages :out]]
                         :catalog []
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions [#_{:flow/from :read-messages
                                            :flow/to [:out]
                                            :flow/short-circuit? true
                                            :flow/predicate ::flow-on?}]
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                            {:kafka/topic topic
                             :kafka/group-id "onyx-consumer-1"
                             :kafka/zookeeper zk-address
                             :kafka/offset-reset :smallest
                             :kafka/force-reset? true
                             :kafka/receive-buffer-bytes 65536
                             :kafka/deserializer-fn ::decompress
                             :onyx/fn ::print-message
                             :onyx/batch-timeout 500
                             :onyx/batch-size batch-size
                             :onyx/max-pending 10000
                             :onyx/min-peers n-partitions
                             :onyx/max-peers n-partitions}))
        (add-task (core-async/output :out 
                                     {:onyx/batch-timeout batch-timeout
                                      :onyx/batch-size batch-size}
                                     100000000 #_(inc (* n-partitions messages-per-partition)))))))

(defn mock-kafka
  "Use a custom version of mock-kafka as opposed to the one in test-utils
  because we need to spawn 2 producers in order to write to each partition"
  [topic zookeeper]
  (let [zk-utils (k-admin/make-zk-utils {:servers [zookeeper]} false)
        _ (k-topics/create-topic! zk-utils topic n-partitions)

        producer-config {:bootstrap.servers ["127.0.0.1:9092"]}
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)
        producer1 (producer/make-producer producer-config key-serializer value-serializer)]
    (time 
     (doseq [p (range n-partitions)]
       (mapv deref 
             (doall (map (fn [x]
                           ;; 116 bytes messages
                           (send-async! producer1 (ProducerRecord. topic p nil (compress {:n x :really-long-string (apply str (repeatedly 30 (fn [] (rand-int 500))))})))) 
                         (range messages-per-partition))))))
    (println "Successfully wrote messages")))

(defn take-until-nothing!
  [ch timeout-ms]
   (loop [ret []]
     (let [tmt (if timeout-ms (timeout timeout-ms) (chan))
           [v c] (alts!! [ch tmt] :priority true)]
       (if (= c tmt)
         ret
         (if (and v (not= v :done))
           (recur (conj ret v))
           (conj ret :done))))))

(deftest ^:benchmark kafka-input-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        {:keys [env-config peer-config]} (read-config (clojure.java.io/resource "config.edn")
                                                      {:profile :bench})
        tenancy-id (str (java.util.UUID/randomUUID))
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        peer-group (onyx.api/start-peer-group peer-config)
        n-peers (+ 2 n-partitions)
        v-peers (onyx.api/start-peers n-peers peer-group)
        zk-address (get-in peer-config [:zookeeper/address])
        job (build-job zk-address test-topic 100 50)
        {:keys [out read-messages]} (get-core-async-channels job)]
    (try
     (println "Topic is " test-topic)
     (mock-kafka test-topic zk-address)
     (Thread/sleep 10000)
     (let [job-ret (onyx.api/submit-job peer-config job)
           _ (println "Job ret" job-ret)
           job-id (:job-id job-ret)
           start-time (System/currentTimeMillis)
           read-nothing-timeout 10000
           read-segments (take-until-nothing! out read-nothing-timeout)]
       (is (= (* n-partitions messages-per-partition) (count read-segments))) 
       (let [run-time (- (System/currentTimeMillis) start-time read-nothing-timeout)
             n-messages-total (* n-partitions messages-per-partition)]
         (println (float (* 1000 (/ n-messages-total run-time))) "messages per second. Processed" n-messages-total "messages in" run-time "ms."))
       (onyx.api/kill-job peer-config job-id))
     (finally 
      (doseq [p v-peers]
        (onyx.api/shutdown-peer p))
      (onyx.api/shutdown-peer-group peer-group)))))
