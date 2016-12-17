(ns onyx.plugin.kafka
  (:require [clojure.core.async :as a :refer [chan >!! <!! close! timeout sliding-buffer]]
            [franzy.admin.cluster :as k-cluster]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.partitions :as k-partitions]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.protocols :refer [send-async! send-sync!]]
            [franzy.clients.producer.types :refer [make-producer-record]]
            [franzy.clients.consumer.protocols :as proto]
            [franzy.clients.consumer.client :as consumer]
            [franzy.common.metadata.protocols :as metadata]
            [franzy.clients.consumer.protocols :refer [assign-partitions! commit-offsets-sync!
                                                       poll! seek-to-offset!] :as cp]
            [onyx.log.curator :as zk]
            [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [taoensso.timbre :as log :refer [fatal info]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.static.uuid :refer [random-uuid]]
            ;; FIXME
            ;[onyx.static.util :refer [kw->fn]]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.extensions :as extensions]
            [onyx.types :as t]
            [onyx.tasks.kafka]
            [schema.core :as s]
            [onyx.api])
  (:import (org.apache.kafka.clients.consumer ConsumerRecords ConsumerRecord)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRebalanceListener Consumer)
           (franzy.clients.consumer.client FranzConsumer)
           [franzy.clients.producer.types ProducerRecord]
           [org.apache.kafka.common TopicPartition]
           [clojure.core.async.impl.channels ManyToManyChannel]))

(def defaults
  {:kafka/receive-buffer-bytes 65536
   :kafka/commit-interval 2000
   :kafka/wrap-with-metadata? false})

(defn seek-offset! [log-prefix consumer kpartition task-map topic checkpoint]
  (let [policy (:kafka/offset-reset task-map)
        start-offsets (:kafka/start-offsets task-map)]
    (cond checkpoint
          (do
           (info log-prefix "Seeking to checkpointed offset at:" checkpoint)
           (seek-to-offset! consumer {:topic topic :partition kpartition} checkpoint))

          start-offsets
          (let [offset (get start-offsets kpartition)]
            (when-not offset
              (throw (ex-info "Offset missing for existing partition when using :kafka/start-offsets" 
                              {:missing-partition kpartition
                               :kafka/start-offsets start-offsets})))
            (seek-to-offset! consumer {:topic topic :partition kpartition} offset))
     
          (= policy :earliest)
          (do
           (info log-prefix "Seeking to earliest offset on topic" {:topic topic :partition kpartition})
           (cp/seek-to-beginning-offset! consumer [{:topic topic :partition kpartition}]))

          (= policy :latest)
          (do
           (info log-prefix "Seeking to latest offset on topic" {:topic topic :partition kpartition})
           (cp/seek-to-end-offset! consumer [{:topic topic :partition kpartition}]))

          :else
          (throw (ex-info "Tried to seek to unknown policy" {:recoverable? false
                                                             :policy policy})))))

;; kafka operations
(defn id->broker [zk-addr]
  (with-open [zk-utils (k-admin/make-zk-utils {:servers zk-addr} false)]
    (reduce
     (fn [result {:keys [id endpoints]}]
       (assoc
        result
        id
        (str (get-in endpoints [:plaintext :host])
             ":"
             (get-in endpoints [:plaintext :port]))))
     {}
     (k-cluster/all-brokers zk-utils))))

(defn find-brokers [zk-addr]
  (let [results (vals (id->broker zk-addr))]
    (if (seq results)
      results
      (throw (ex-info "Could not locate any Kafka brokers to connect to."
                      {:recoverable? true
                       :zk-addr zk-addr})))))

(defn check-num-peers-equals-partitions 
  [{:keys [onyx/min-peers onyx/max-peers onyx/n-peers kafka/partition] :as task-map} n-partitions]
  (let [fixed-partition? (and partition (or (= 1 n-peers)
                                            (= 1 max-peers)))
        all-partitions-covered? (or (= n-partitions min-peers max-peers)
                                    (= 1 n-partitions max-peers)
                                    (= n-partitions n-peers))] 
    (when-not (or fixed-partition? all-partitions-covered?)
      (let [e (ex-info ":onyx/min-peers must equal :onyx/max-peers and the number of partitions, or :onyx/n-peers must equal number of kafka partitions" 
                       {:n-partitions n-partitions 
                        :n-peers n-peers
                        :min-peers min-peers
                        :max-peers max-peers
                        :recoverable? false
                        :task-map task-map})] 
        (log/error e)
        (throw e)))))

(defn start-kafka-consumer
  [event lifecycle]
  {})

(defn take-record! [^java.util.Iterator iterator]
  (if (.hasNext iterator)
    (.next iterator)))

(defrecord KafkaReadMessages 
  [log-prefix task-map topic kpartition batch-timeout deserializer-fn segment-fn consumer iter record offset drained?]

  p/Plugin
  (start [this event]
    (let [{:keys [kafka/group-id kafka/consumer-opts]} task-map
        brokers (find-brokers (:kafka/zookeeper task-map))
        _ (s/validate onyx.tasks.kafka/KafkaInputTaskMap task-map)
        consumer-config (merge {:bootstrap.servers (find-brokers (:kafka/zookeeper task-map))
                                :group.id group-id
                                :enable.auto.commit false
                                :receive.buffer.bytes (or (:kafka/receive-buffer-bytes task-map)
                                                          (:kafka/receive-buffer-bytes defaults))
                                :auto.offset.reset (:kafka/offset-reset task-map)}
                               consumer-opts)
        key-deserializer (byte-array-deserializer)
        value-deserializer (byte-array-deserializer)
        consumer (consumer/make-consumer consumer-config key-deserializer value-deserializer)
        kpartition (if-let [part (:partition task-map)]
                     (Integer/parseInt part)
                     (:slot-id event)) ;; FIXME, will be back to onyx.core/slot-id
        partitions (mapv :partition (metadata/partitions-for consumer topic))
        n-partitions (count partitions)
        _ (check-num-peers-equals-partitions task-map n-partitions)
        _ (assign-partitions! consumer [{:topic topic :partition kpartition}])]
      (assoc this :consumer consumer :kpartition kpartition)))

  (stop [this event] this
    (when consumer (.close consumer)))

  i/Input
  (checkpoint [this]
    offset)

  (recover [this replica-version checkpoint]
    (reset! drained? false)
    (seek-offset! log-prefix consumer kpartition task-map topic checkpoint)
    (assoc this :offset nil))

  (segment [this]
    (let [v (some-> record segment-fn)]
      (if-not (= v :done)
        v)))

  (next-epoch [this epoch]
    this)

  (next-state [this state]
    (let [_ (when (or (nil? @iter)
                      (not (.hasNext ^java.util.Iterator @iter)))
              (reset! iter (.iterator ^ConsumerRecords (.poll ^Consumer (.consumer ^FranzConsumer consumer) batch-timeout))))
          rec (take-record! @iter)
          new-offset (if rec
                       (.offset rec)
                       offset)]
      ;; Doubling up on the deserialization for now
      ;; will remove done soon
      (if (= :done (some-> rec (.value) deserializer-fn))
        (reset! drained? true))
      (-> this
          (assoc :record rec)
          (assoc :offset new-offset))))

  (completed? [this]
    @drained?))

(defn read-messages [{:keys [task-map log-prefix]}]
  (let [{:keys [kafka/topic kafka/deserializer-fn]} task-map ;; fixme onyx.core
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        drained? (atom false)
        wrap-message? (or (:kafka/wrap-with-metadata? task-map) (:kafka/wrap-with-metadata? defaults))
        deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))
        segment-fn (if wrap-message?
                     (fn [^ConsumerRecord cr]
                       {:topic (.topic cr)
                        :partition (.partition cr)
                        :key (.key cr)
                        :message (deserializer-fn (.value cr))
                        :offset (.offset cr)})
                     (fn [^ConsumerRecord cr]
                       (deserializer-fn (.value cr))))]
    (->KafkaReadMessages log-prefix task-map topic nil batch-timeout
                         deserializer-fn segment-fn nil (atom nil) nil nil drained?)))

(defn close-read-messages
  [{:keys [kafka/retry-ch kafka/commit-fut kafka/consumer] :as pipeline} lifecycle]
  {})

(defn inject-write-messages
  [{:keys [onyx.core/pipeline] :as pipeline} lifecycle]
  {})

(defn close-write-resources
  [event lifecycle]
  {})

(defn- message->producer-record
  [serializer-fn topic m]
  (let [message (:message m)
        k (some-> m :key serializer-fn)
        p (some-> m :partition int)
        message-topic (get m :topic topic)]
    (cond (not (contains? m :message))
          (throw (ex-info "Payload is missing required. Need message key :message"
                          {:recoverable? false
                           :payload m}))

          (nil? message-topic)
          (throw (ex-info
                  (str "Unable to write message payload to Kafka! "
                       "Both :kafka/topic, and :topic in message payload "
                       "are missing!")
                  {:recoverable? false
                   :payload m}))
          :else
          (ProducerRecord. message-topic p k (serializer-fn message)))))

(defrecord KafkaWriteMessages [task-map config topic producer serializer-fn]
  p/Plugin
  (start [this event] 
    ;; move producer creation to in here
    this)

  (stop [this event] 
    (.close producer)
    this)

  o/Output
  (prepare-batch
    [_ state]
    state)

  (write-batch
    [_ state]
    (let [{:keys [results]} (get-event state)]
      ;; todo, write version that doesn't block?
      (let [messages (mapcat :leaves (:tree results))]
        ;(println "Writing messages" messages)
        (->> messages
             (map (fn [msg]
                    (->> (:message msg)
                         (message->producer-record serializer-fn topic)
                         (send-async! producer))))
             (doall)
             (run! deref))
        (advance state)))))

(defn write-messages [{:keys [task-map] :as event}]
  (let [_ (s/validate onyx.tasks.kafka/KafkaOutputTaskMap task-map)
        request-size (or (get task-map :kafka/request-size) (get defaults :kafka/request-size))
        producer-opts (:kafka/producer-opts task-map)
        config (merge {:bootstrap.servers (vals (id->broker (:kafka/zookeeper task-map)))
                       :max.request.size request-size}
                      producer-opts)
        topic (:kafka/topic task-map)
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)
        producer (producer/make-producer config key-serializer value-serializer)
        serializer-fn (kw->fn (:kafka/serializer-fn task-map))]
    (->KafkaWriteMessages task-map config topic producer serializer-fn)))

(defn read-handle-exception [event lifecycle lf-kw exception]
  (if (false? (:recoverable? (ex-data exception)))
    :kill
    :restart))

(def read-messages-calls
  {:lifecycle/before-task-start start-kafka-consumer
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-messages})

(defn write-handle-exception [event lifecycle lf-kw exception]
  (if (false? (:recoverable? (ex-data exception)))
    :kill
    :restart))

(def write-messages-calls
  {:lifecycle/before-task-start inject-write-messages
   :lifecycle/handle-exception write-handle-exception
   :lifecycle/after-task-stop close-write-resources})
