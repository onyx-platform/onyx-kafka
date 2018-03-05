(ns onyx.kafka.helpers
  (:require [clojure.string :as string]
            [taoensso.timbre :as log])
  (:import [kafka.utils ZkUtils]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [org.apache.kafka.clients.producer KafkaProducer Callback ProducerRecord]
           [org.apache.kafka.common.serialization ByteArrayDeserializer ByteArraySerializer Serializer Deserializer]
           [org.apache.kafka.common TopicPartition PartitionInfo]
           [kafka.admin AdminUtils]
           [kafka.utils ZKStringSerializer$]
           [org.I0Itec.zkclient ZkClient ZkConnection IZkConnection]
           [org.I0Itec.zkclient.serialize ZkSerializer]
           [scala.collection JavaConversions]
           [java.util Properties]))

(def zk-defaults
  {:session-timeout 30000
   :connection-timeout 30000
   :operation-retry-timeout (long -1)
   :serializer (ZKStringSerializer$/MODULE$)})

(defn munge-servers [servers]
  (if (coll? servers)
    (string/join "," servers)
    servers))

(defn as-properties ^Properties [m]
  (let [ps (Properties.)]
    (doseq [[k v] m] (.setProperty ps k (str v)))
    ps))

(defn as-java [opts]
  (cond-> opts
    (get opts "bootstrap.servers") (update "bootstrap.servers" munge-servers)))

(defn make-zk-connection [zk-config]
  (let [{:keys [servers session-timeout]} zk-config
        server-seq (if (coll? servers) (string/join "," servers) servers)]
    (ZkConnection. server-seq session-timeout)))

(defn make-zk-client [zk-connection zk-config]
  (let [{:keys [connection-timeout operation-retry-timeout serializer]} zk-config]
    (ZkClient. ^IZkConnection zk-connection
               (int connection-timeout)
               ^ZkSerializer serializer
               (long operation-retry-timeout))))

(defn make-zk-utils ^ZkUtils [zk-config secure?]
  (let [merged-config (merge zk-defaults zk-config)
        session-timeout (:session-timeout merged-config)
        connection-timeout (:connection-timeout merged-config)
        zk-connection (make-zk-connection merged-config)
        zk-client (make-zk-client zk-connection merged-config)]
    (ZkUtils. zk-client zk-connection secure?)))

(defn all-brokers
  [^ZkUtils zk-utils]
  (->> (.getAllBrokersInCluster zk-utils)
       (JavaConversions/bufferAsJavaList)
       (map
        (fn [broker]
          {:id (.id broker)
           :endpoints
           (map
            (fn [endpoint]
              {:host (.host endpoint)
               :port (.port endpoint)
               :protocol-type (.name (.securityProtocol endpoint))})
            (JavaConversions/seqAsJavaList (.endPoints broker)))}))))

(defn id->broker [zk-addr]
  (with-open [zk-utils (make-zk-utils {:servers zk-addr} false)]
    (reduce
     (fn [result {:keys [id endpoints]}]
       (assoc
        result
        id
        (str (:host (first endpoints)) ":" (:port (first endpoints)))))
     {}
     (all-brokers zk-utils))))

(defn byte-array-serializer []
  (ByteArraySerializer.))

(defn byte-array-deserializer []
  (ByteArrayDeserializer.))

(defn ^KafkaProducer build-producer [producer-opts key-serializer value-serializer]
  (KafkaProducer. ^Properties (as-properties (as-java producer-opts))
                  ^Serializer key-serializer
                  ^Serializer value-serializer))

(defn ^KafkaConsumer build-consumer [consumer-opts key-deserializer value-deserializer]
  (KafkaConsumer. ^Properties (as-properties (as-java consumer-opts))
                  ^Deserializer key-deserializer
                  ^Deserializer value-deserializer))

(defn partitions-for-topic [consumer topic]
  (let [parts (.partitionsFor ^KafkaConsumer consumer topic)]
    (map
     (fn [part]
       {:topic (.topic part)
        :partition (.partition part)})
     parts)))

(defn to-topic-partition [tp]
  (TopicPartition. (:topic tp) (:partition tp)))

(defn assign-partitions! [consumer topic-partitions]
  (->> topic-partitions
       (mapv to-topic-partition)
       (.assign ^KafkaConsumer consumer)))

(defn seek-to-offset! [consumer topic-partition offset]
  (let [encoded (to-topic-partition topic-partition)]
    (.seek ^KafkaConsumer consumer encoded offset)))

(defn seek-to-beginning! [consumer topic-partitions]
  (let [encoded (map to-topic-partition topic-partitions)]
    (.seekToBeginning ^KafkaConsumer consumer encoded)))

(defn seek-to-end! [consumer topic-partitions]
  (let [encoded (map to-topic-partition topic-partitions)]
    (.seekToEnd ^KafkaConsumer consumer encoded)))

(defn consumer-record->message
  [decompress-fn ^ConsumerRecord m]
  {:key (some-> m (.key) decompress-fn)
   :partition (.partition m)
   :topic (.topic m)
   :value (-> m (.value) decompress-fn)
   :timestamp (.timestamp m)})

(defn poll! [consumer timeout]
  (.poll ^KafkaConsumer consumer timeout))

(defn take-now
  "Reads whatever it can from a topic on the assumption that we've distributed
  work across multiple topics and another topic contained :done."
  ([bootstrap-servers topic decompress-fn]
   (take-now bootstrap-servers topic decompress-fn 5000))
  ([bootstrap-servers topic decompress-fn timeout]
   (log/info {:msg "Taking now..." :topic topic})
   (let [c (build-consumer {"bootstrap.servers" bootstrap-servers} (byte-array-deserializer) (byte-array-deserializer))
         topic-partitions [{:topic topic :partition 0}]]
     (assign-partitions! c topic-partitions)
     (seek-to-beginning! c topic-partitions)
     (mapv #(consumer-record->message decompress-fn %) (poll! c timeout)))))

(defn create-topic! [zk-addr topic-name num-partitions replication-factor]
  (with-open [zk-utils (make-zk-utils {:servers zk-addr} false)]
    (AdminUtils/createTopic
     zk-utils topic-name num-partitions replication-factor (as-properties {})
     (kafka.admin.RackAwareMode$Safe$.))))

(deftype ProducerCallback [p]
  Callback
  (onCompletion [_ v exception]
    (deliver p true)))

(defn send-sync! [producer topic part k v]
  (let [p (promise)
        record (ProducerRecord. topic part k v)]
    (.send producer record (->ProducerCallback p))
    @p))

(defn partition-info->topic-partition [topic ^PartitionInfo part-info]
  (TopicPartition. topic (.partition part-info)))

(defn end-offsets [bootstrap-servers topic]
  (let [opts {"bootstrap.servers" bootstrap-servers}
        k-deser (ByteArrayDeserializer.)
        v-deser (ByteArrayDeserializer.)]
    (with-open [consumer (build-consumer opts k-deser v-deser)]
      (let [parts (.partitionsFor consumer topic)
            tps (map (partial partition-info->topic-partition topic) parts)]
        (.endOffsets consumer tps)))))

(defn end-offsets->clj [end-offsets]
  (reduce-kv
   (fn [all ^TopicPartition k v]
     (assoc all (.partition k) v))
   {}
   (into {} end-offsets)))
