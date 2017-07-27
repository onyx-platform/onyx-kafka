(ns onyx.plugin.kafka
  (:require [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [onyx.plugin.partition-assignment :refer [partitions-for-slot]]
            [onyx.kafka.helpers :as h]
            [taoensso.timbre :as log :refer [fatal info]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.plugin.protocols :as p]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.kafka]
            [schema.core :as s]
            [onyx.api])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.apache.kafka.clients.consumer ConsumerRecords ConsumerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRebalanceListener Consumer]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]))

(def defaults
  {:kafka/receive-buffer-bytes 65536
   :kafka/commit-interval 2000
   :kafka/wrap-with-metadata? false
   :kafka/unable-to-find-broker-backoff-ms 8000})

(defn seek-offset! [log-prefix consumer kpartitions task-map topic checkpoint]
  (let [policy (:kafka/offset-reset task-map)
        start-offsets (:kafka/start-offsets task-map)]
    (doseq [kpartition kpartitions]
      (cond (get checkpoint kpartition)
            (let [offset (get checkpoint kpartition)]
              (info log-prefix "Seeking to checkpointed offset at:" (inc offset))
              (h/seek-to-offset! consumer {:topic topic :partition kpartition} (inc offset)))

            start-offsets
            (let [offset (get start-offsets kpartition)]
              (when-not offset
                (throw (ex-info "Offset missing for existing partition when using :kafka/start-offsets"
                                {:missing-partition kpartition
                                 :kafka/start-offsets start-offsets})))
              (h/seek-to-offset! consumer {:topic topic :partition kpartition} offset))

            (= policy :earliest)
            (do
              (info log-prefix "Seeking to earliest offset on topic" {:topic topic :partition kpartition})
              (h/seek-to-beginning! consumer [{:topic topic :partition kpartition}]))

            (= policy :latest)
            (do
              (info log-prefix "Seeking to latest offset on topic" {:topic topic :partition kpartition})
              (h/seek-to-end! consumer [{:topic topic :partition kpartition}]))

            :else
            (throw (ex-info "Tried to seek to unknown policy" {:recoverable? false
                                                               :policy policy}))))))

(defn find-brokers [task-map]
  (let [zk-addr (:kafka/zookeeper task-map)
        results (vals (h/id->broker zk-addr))]
    (if (seq results)
      results
      (do
        (info "Could not locate any Kafka brokers to connect to. Backing off.")
        (Thread/sleep (or (:kafka/unable-to-find-broker-backoff-ms task-map) 
                          (:kafka/unable-to-find-broker-backoff-ms defaults)))
        (throw (ex-info "Could not locate any Kafka brokers to connect to."
                        {:recoverable? true
                         :zk-addr zk-addr}))))))

(defn start-kafka-consumer
  [event lifecycle]
  {})

(defn check-num-peers-equals-partitions 
  [{:keys [onyx/min-peers onyx/max-peers onyx/n-peers kafka/partition] :as task-map} n-partitions]
  (let [fixed-partition? (and partition (or (= 1 n-peers)
                                            (= 1 max-peers)))
        fixed-npeers? (or (= min-peers max-peers) (= 1 max-peers)
                          (and n-peers (and (not min-peers) (not max-peers))))
        n-peers (or max-peers n-peers)
        n-peers-less-eq-n-partitions (<= n-peers n-partitions)] 
    (when-not (or fixed-partition? fixed-npeers? n-peers-less-eq-n-partitions)
      (let [e (ex-info ":onyx/min-peers must equal :onyx/max-peers, or :onyx/n-peers must be set, and :onyx/min-peers and :onyx/max-peers must not be set. Number of peers should also be less than or equal to the number of partitions."
                       {:n-partitions n-partitions 
                        :n-peers n-peers
                        :min-peers min-peers
                        :max-peers max-peers
                        :recoverable? false
                        :task-map task-map})] 
        (log/error e)
        (throw e)))))

(defn assign-partitions-to-slot! [consumer* task-map topic n-partitions slot]
  (if-let [part (:partition task-map)]
    (let [p (Integer/parseInt part)]
      (h/assign-partitions! consumer* [{:topic topic :partition p}])
      [p])
    (let [n-slots (or (:onyx/n-peers task-map) (:onyx/max-peers task-map))
          [lower upper] (partitions-for-slot n-partitions n-slots slot)
          parts-range (range lower (inc upper))
          parts (map (fn [p] {:topic topic :partition p}) parts-range)]
      (h/assign-partitions! consumer* parts)
      parts-range)))

(deftype KafkaReadMessages 
    [log-prefix task-map topic ^:unsynchronized-mutable kpartitions batch-timeout
     deserializer-fn segment-fn read-offset ^:unsynchronized-mutable consumer 
     ^:unsynchronized-mutable iter ^:unsynchronized-mutable partition->offset ^:unsynchronized-mutable drained]
  p/Plugin
  (start [this event]
    (let [{:keys [kafka/group-id kafka/consumer-opts]} task-map
          brokers (find-brokers task-map)
          _ (s/validate onyx.tasks.kafka/KafkaInputTaskMap task-map)
          consumer-config (merge {"bootstrap.servers" brokers
                                  "group.id" (or group-id "onyx")
                                  "enable.auto.commit" false
                                  "receive.buffer.bytes" (or (:kafka/receive-buffer-bytes task-map)
                                                             (:kafka/receive-buffer-bytes defaults))
                                  "auto.offset.reset" (name (:kafka/offset-reset task-map))}
                                 consumer-opts)
          _ (info log-prefix "Starting kafka/read-messages task with consumer opts:" consumer-config)
          key-deserializer (h/byte-array-deserializer)
          value-deserializer (h/byte-array-deserializer)
          consumer* (h/build-consumer consumer-config key-deserializer value-deserializer)
          partitions (mapv :partition (h/partitions-for-topic consumer* topic))
          n-partitions (count partitions)]
      (check-num-peers-equals-partitions task-map n-partitions)
      (let [kpartitions* (assign-partitions-to-slot! consumer* task-map topic n-partitions (:onyx.core/slot-id event))]
        (set! consumer consumer*)
        (set! kpartitions kpartitions*)
        this)))

  (stop [this event] 
    (when consumer 
      (.close ^KafkaConsumer consumer)
      (set! consumer nil))
    this)

  p/Checkpointed
  (checkpoint [this]
    partition->offset)

  (recover! [this replica-version checkpoint]
    (set! drained false)
    (set! iter nil)
    (set! partition->offset checkpoint)
    (seek-offset! log-prefix consumer kpartitions task-map topic checkpoint)
    this)

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    drained)

  p/Input
  (poll! [this _ remaining-ms]
    (if (and iter (.hasNext ^java.util.Iterator iter))
      (let [rec ^ConsumerRecord (.next ^java.util.Iterator iter)
            deserialized (some-> rec segment-fn)]
        (cond (= :done deserialized)
              (do (set! drained true)
                  nil)
              deserialized
              (let [new-offset (.offset rec)
                    part (.partition rec)]
                (.set ^AtomicLong read-offset new-offset)
                (set! partition->offset (assoc partition->offset part new-offset))
                deserialized)))
      (do (set! iter (.iterator ^ConsumerRecords (.poll ^Consumer consumer remaining-ms)))
          nil))))

(defn read-messages [{:keys [onyx.core/task-map onyx.core/log-prefix onyx.core/monitoring] :as event}]
  (let [{:keys [kafka/topic kafka/deserializer-fn]} task-map
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        wrap-message? (or (:kafka/wrap-with-metadata? task-map) (:kafka/wrap-with-metadata? defaults))
        deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))
        key-deserializer-fn (if-let [kw (:kafka/key-deserializer-fn task-map)] (kw->fn kw) identity)
        segment-fn (if wrap-message?
                     (fn [^ConsumerRecord cr]
                       {:topic (.topic cr)
                        :partition (.partition cr)
                        :key (when-let [k (.key cr)] (key-deserializer-fn k))
                        :message (deserializer-fn (.value cr))
                        :serialized-key-size (.serializedKeySize cr)
                        :serialized-value-size (.serializedValueSize cr)
                        :timestamp (.timestamp cr)
                        :offset (.offset cr)})
                     (fn [^ConsumerRecord cr]
                       (deserializer-fn (.value cr))))
        read-offset (:read-offset monitoring)]
    (->KafkaReadMessages log-prefix task-map topic nil batch-timeout
                         deserializer-fn segment-fn read-offset nil nil nil false)))

(defn close-read-messages
  [event lifecycle]
  {})

(defn inject-write-messages
  [event lifecycle]
  {})

(defn close-write-resources
  [event lifecycle]
  {})

(defn- message->producer-record
  [key-serializer-fn serializer-fn topic kpartition m]
  (let [message (:message m)
        k (some-> m :key key-serializer-fn)
        message-topic (get m :topic topic)
        message-partition (some-> m (get :partition kpartition) int)]
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
          (ProducerRecord. message-topic message-partition k (serializer-fn message)))))

(defn clear-write-futures! [fs]
  (doall (remove (fn [f] 
                   (assert (not (.isCancelled ^java.util.concurrent.Future f)))
                   (realized? f)) 
                 fs)))

(defrecord KafkaWriteMessages [task-map config topic kpartition producer key-serializer-fn serializer-fn write-futures exception write-callback]
  p/Plugin
  (start [this event] 
    this)

  (stop [this event] 
    (.close ^KafkaProducer producer)
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    (when @exception (throw @exception))
    (empty? (vswap! write-futures clear-write-futures!)))

  (completed? [this]
    (when @exception (throw @exception))
    (empty? (vswap! write-futures clear-write-futures!)))

  p/Checkpointed
  (recover! [this _ _] 
    this)

  (checkpoint [this])

  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica _]
    true)

  (write-batch [this {:keys [onyx.core/results]} replica _]
    (when @exception (throw @exception))
    (vswap! write-futures
            (fn [fs]
              (-> fs
                  (clear-write-futures!)
                  (into (comp (mapcat :leaves)
                              (map
                               (fn [msg]
                                 (let [record (message->producer-record key-serializer-fn serializer-fn topic kpartition msg)]
                                   (.send ^KafkaProducer producer record write-callback)))))
                        (:tree results)))))
    true))

(def write-defaults {:kafka/request-size 307200})

(deftype ExceptionCallback [e]
  Callback
  (onCompletion [_ v exception]
    (when exception (reset! e exception))))

(defn write-messages [{:keys [onyx.core/task-map onyx.core/log-prefix] :as event}]
  (let [_ (s/validate onyx.tasks.kafka/KafkaOutputTaskMap task-map)
        request-size (or (get task-map :kafka/request-size) (get write-defaults :kafka/request-size))
        producer-opts (:kafka/producer-opts task-map)
        config (merge {"bootstrap.servers" (vals (h/id->broker (:kafka/zookeeper task-map)))
                       "max.request.size" request-size}
                      producer-opts)
        _ (info log-prefix "Starting kafka/write-messages task with producer opts:" config)
        topic (:kafka/topic task-map)
        kpartition (:kafka/partition task-map)
        key-serializer (h/byte-array-serializer)
        value-serializer (h/byte-array-serializer)
        producer (h/build-producer config key-serializer value-serializer)
        serializer-fn (kw->fn (:kafka/serializer-fn task-map))
        key-serializer-fn (if-let [kw (:kafka/key-serializer-fn task-map)] (kw->fn kw) identity)
        exception (atom nil)
        write-callback (->ExceptionCallback exception)
        write-futures (volatile! (list))]
    (->KafkaWriteMessages task-map config topic kpartition producer
                          key-serializer-fn serializer-fn
                          write-futures exception write-callback)))

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
