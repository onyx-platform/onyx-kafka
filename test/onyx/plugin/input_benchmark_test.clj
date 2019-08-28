(ns onyx.plugin.input-benchmark-test
  (:require [clojure.core.async :refer [<!! go pipe timeout chan alts!!]]
            [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [aero.core :refer [read-config]]
            [taoensso.nippy :as nip]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.helpers :as h]
            [onyx.tasks.kafka :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api])
  (:import [org.apache.kafka.clients.producer KafkaProducer Callback ProducerRecord]))

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

(def messages-per-partition 1000000)
(def n-partitions 1)

(defn print-message [segment]
  segment)

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
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                            {:kafka/topic topic
                             :kafka/group-id "onyx-consumer-1"
                             :kafka/zookeeper zk-address
                             :kafka/offset-reset :earliest
                             :kafka/receive-buffer-bytes 65536
                             :kafka/deserializer-fn ::decompress
                             :onyx/fn ::print-message
                             :onyx/batch-timeout 50
                             :onyx/batch-size batch-size
                             :onyx/min-peers n-partitions
                             :onyx/max-peers n-partitions}))
        (add-task (core-async/output :out 
                                     {:onyx/batch-timeout batch-timeout
                                      :onyx/batch-size batch-size}
                                     100000000)))))

(defn write-data
  [topic zookeeper bootstrap-servers]
  (h/create-topic! zookeeper topic n-partitions 1)
  (let [producer-config {"bootstrap.servers" bootstrap-servers
                         "key.serializer" (h/byte-array-serializer-name)
                         "value.serializer" (h/byte-array-serializer-name)}
        producer1 (h/build-producer producer-config)]
    (time 
     (doseq [p (range n-partitions)]
       (mapv deref 
             (doall
              (map (fn [x]
                     ;; 116 bytes messages
                     (.send producer1 (ProducerRecord. topic 
                                                       (int p) 
                                                       nil
                                                       (compress {:n x :really-long-string (apply str (repeatedly 30 (fn [] (rand-int 500))))})))) 
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
        {:keys [env-config peer-config test-config]} (read-config (clojure.java.io/resource "config.edn")
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
     (write-data test-topic zk-address (:kafka-bootstrap test-config))
     ;; Appropriate time to settle before submitting the job
     (Thread/sleep 5000)
     (let [job-ret (onyx.api/submit-job peer-config job)
           _ (println "Job ret" job-ret)
           job-id (:job-id job-ret)
           start-time (System/currentTimeMillis)
           read-nothing-timeout 30000
           read-segments (take-until-nothing! out read-nothing-timeout)]
       (is (= (* n-partitions messages-per-partition) (count read-segments))) 
       (let [run-time (- (System/currentTimeMillis) start-time read-nothing-timeout)
             n-messages-total (* n-partitions messages-per-partition)]
         (println (float (* 1000 (/ n-messages-total run-time))) 
                  "messages per second. Processed" n-messages-total 
                  "messages in" run-time "ms."))
       (onyx.api/kill-job peer-config job-id))
     (finally 
      (doseq [p v-peers]
        (onyx.api/shutdown-peer p))
      (onyx.api/shutdown-peer-group peer-group)))))
