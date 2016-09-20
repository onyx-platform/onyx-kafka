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
            [taoensso.timbre :as log :refer [fatal info]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.util :refer [kw->fn]]
            [onyx.extensions :as extensions]
            [onyx.types :as t]
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

(defn get-offset-reset [task-map x]
  (if (:kafka/force-reset? task-map)
    ({:smallest :earliest
      :largest :latest}
     x)
    :none))

(defn checkpoint-name [group-id topic assigned-partition]
  (format "%s-%s-%s" group-id topic assigned-partition))

(defn checkpoint-or-beginning [log consumer topic kpartition group-id task-map]
  (let [k (checkpoint-name group-id topic kpartition)]
    (try
      (let [offset (inc (:offset (extensions/read-chunk log :chunk k)))]
        (seek-to-offset! consumer {:topic topic :partition kpartition} offset))
      (catch org.apache.zookeeper.KeeperException$NoNodeException nne
        (if-let [start-offsets (:kafka/start-offsets task-map)]
          (let [offset (get start-offsets kpartition)]
            (when-not offset
              (throw (ex-info "Offset missing for existing partition when using :kafka/start-offsets" 
                              {:missing-partition kpartition
                               :recoverable? false
                               :kafka/start-offsets start-offsets})))
            (seek-to-offset! consumer {:topic topic :partition kpartition} offset))
          (cp/seek-to-beginning-offset! consumer [{:topic topic :partition kpartition}]))))))

(defn seek-offset! [log consumer group-id topic kpartition task-map]
  (if (and (:kafka/force-reset? task-map)
           (not (:kafka/start-offset task-map)))
    (let [policy (:kafka/offset-reset task-map)]
      (cond (= policy :smallest)
            (cp/seek-to-beginning-offset! consumer [{:topic topic :partition kpartition}])

            (= policy :largest)
            (cp/seek-to-end-offset! consumer [{:topic topic :partition kpartition}])

            :else
            (throw (ex-info "Tried to seek to unknown policy" {:recoverable? false
                                                               :policy policy}))))
    (checkpoint-or-beginning log consumer topic kpartition group-id task-map)))

(defn log-id [replica-val job-id peer-id task-id]
  (get-in replica-val [:task-slot-ids job-id task-id peer-id]))

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

(defn highest-offset-to-commit [offsets]
  (->> (sort offsets)
       (partition-all 2 1)
       (partition-by #(- (or (second %) (first %)) (first %)))
       (first)
       (last)
       (last)))

(defn commit-loop [log group-id topic kpartition commit-interval pending-commits]
  (try
    (loop []
      (Thread/sleep commit-interval)
      (when-let [offset (highest-offset-to-commit @pending-commits)]
        (let [k (checkpoint-name group-id topic kpartition)
              data {:offset offset}]
          (extensions/force-write-chunk log :chunk data k)
          (swap! pending-commits (fn [coll] (remove (fn [k] (<= k offset)) coll)))))
      (when-not (Thread/interrupted) 
        (recur)))
    (catch InterruptedException e
      (throw e))
    (catch Throwable e
      (fatal e)))) 

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
  [{:keys [onyx.core/task-map onyx.core/pipeline onyx.core/log onyx.core/replica] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id kafka/consumer-opts]} task-map
        brokers (find-brokers (:kafka/zookeeper task-map))
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        client-id "onyx"
        retry-ch (:retry-ch pipeline)
        pending-messages (:pending-messages pipeline)
        pending-commits (:pending-commits pipeline)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)
        consumer-config (merge
                         {:bootstrap.servers (find-brokers (:kafka/zookeeper task-map))
                          :group.id group-id
                          :enable.auto.commit false
                          :receive.buffer.bytes (or (:kafka/receive-buffer-bytes task-map)
                                                    (:kafka/receive-buffer-bytes defaults))
                          :auto.offset.reset (get-offset-reset task-map (:kafka/offset-reset task-map))}
                         consumer-opts)
        key-deserializer (byte-array-deserializer)
        value-deserializer (byte-array-deserializer)
        consumer (consumer/make-consumer consumer-config key-deserializer value-deserializer)
        kpartition (if partition
                     (Integer/parseInt (str partition))
                     (log-id @replica job-id peer-id task-id))
        partitions (mapv :partition (metadata/partitions-for consumer topic))
        n-partitions (count partitions)
        done-unsupported? (and (> (count partitions) 1)
                               (not (:kafka/partition task-map)))
        _ (check-num-peers-equals-partitions task-map n-partitions)
        _ (assign-partitions! consumer [{:topic topic :partition kpartition}])
        _ (seek-offset! log consumer group-id topic kpartition task-map)
        offset (cp/next-offset consumer {:topic topic :partition kpartition})
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        commit-fut (future (commit-loop log group-id topic kpartition commit-interval pending-commits))]
    {:kafka/commit-fut commit-fut
     :kafka/retry-ch retry-ch
     :kafka/consumer consumer
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits
     :kafka/done-unsupported? done-unsupported?}))

(defn all-done? [messages]
  (empty? 
   (remove #(= :done (:message %))
           messages)))

(defn take-values! [batch ch n-messages]
  (loop [n 0]
    (if-let [v (if (< n n-messages)
                   (a/poll! ch))]
      (do (conj! batch v)
          (recur (inc n)))
      n)))

(defn add-pending-batch [pending-messages batch]
  (persistent! 
   (reduce (fn [p m]
             (assoc! p (:id m) m))
           (transient pending-messages) 
           batch)))

(defn take-records! [batch ^java.util.Iterator iterator segment-fn n-messages]
  (loop [n n-messages]
    (if (and (.hasNext iterator)
             (pos? n))
      (do 
       (conj! batch (segment-fn (.next iterator)))
       (recur (dec n))))))

(defrecord KafkaReadMessages
  [task-map max-pending batch-size batch-timeout pending-messages pending-commits drained? iter retry-ch segment-fn]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (let [pending (count @pending-messages)
          max-segments (max (min (- max-pending pending) batch-size) 0)
          _ (when (and (not (zero? max-segments)) 
                       (or (nil? @iter)
                           (not (.hasNext ^java.util.Iterator @iter))))
              (reset! iter (.iterator ^ConsumerRecords (.poll ^Consumer (.consumer ^FranzConsumer (:kafka/consumer event)) batch-timeout))))
          batch (transient [])
          n-retries (take-values! batch retry-ch batch-size)
          _ (take-records! batch @iter segment-fn (- max-segments n-retries))
          batch (persistent! batch)]
      (swap! pending-messages add-pending-batch batch)
      (when (and (all-done? batch)
                 (all-done? (vals @pending-messages))
                 (zero? (count (.buf ^ManyToManyChannel retry-ch)))
                 (or (not (empty? @pending-messages))
                     (not (empty? batch))))
        (if (:kafka/done-unsupported? event)
          (throw (ex-info ":done is not supported for auto assigned kafka partitions. (:kafka/partition must be supplied)"
                          {:recoverable? false
                           :task-map task-map}))
          (reset! drained? true)))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (when-let [offset (:offset (get @pending-messages segment-id))]
      (swap! pending-commits conj offset))
    (swap! pending-messages dissoc segment-id))

  (retry-segment
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! retry-ch (t/input (random-uuid) (:message msg)))
      (swap! pending-messages dissoc segment-id)))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn read-messages [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        retry-ch (chan (* 2 max-pending))
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        drained? (atom false)
        wrap-message? (or (:kafka/wrap-with-metadata? task-map) (:kafka/wrap-with-metadata? defaults))
        deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))
        segment-fn (if wrap-message?
                     (fn [^ConsumerRecord cr]
                       (assoc (t/input (random-uuid)
                                       {:topic (.topic cr)
                                        :partition (.partition cr)
                                        :key (.key cr)
                                        :message (deserializer-fn (.value cr))
                                        :offset (.offset cr)})
                              :offset (.offset cr)))
                     (fn [^ConsumerRecord cr]
                       (assoc (t/input (random-uuid) 
                                       (deserializer-fn (.value cr)))
                              :offset (.offset cr))))
        buffered-segments (atom nil)]
    (->KafkaReadMessages task-map max-pending batch-size batch-timeout
                         pending-messages pending-commits drained? buffered-segments retry-ch segment-fn)))

(defn close-read-messages
  [{:keys [kafka/retry-ch kafka/commit-fut kafka/consumer] :as pipeline} lifecycle]
  (future-cancel commit-fut)
  (.close consumer)
  (close! retry-ch)
  (while (a/poll! retry-ch))
  {})

(defn inject-write-messages
  [{:keys [onyx.core/pipeline] :as pipeline} lifecycle]
  {:kafka/config (:config pipeline)
   :kafka/topic (:topic pipeline)
   :kafka/serializer-fn (:serializer-fn pipeline)
   :kafka/producer (:producer pipeline)})

(defn close-write-resources
  [event lifecycle]
  (.close (:kafka/producer event)))

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
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [this {:keys [onyx.core/results]}]
    (let [messages (mapcat :leaves (:tree results))]
      (doall
       (->> messages
            (map (fn [msg]
                   (send-async! producer (message->producer-record serializer-fn topic (:message msg)))))
            (doall)
            (run! deref)))
      {}))

  (seal-resource
    [_ {:keys [onyx.core/results]}]
    (if (:kafka/no-seal? task-map)
      {}
      (send-sync! producer (ProducerRecord. topic nil nil (serializer-fn :done))))))

(defn write-messages [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        request-size (or (get task-map :kafka/request-size) (get defaults :kafka/request-size))
        producer-opts (:kafka/producer-opts task-map)
        config (merge
                {:bootstrap.servers (vals (id->broker (:kafka/zookeeper task-map)))
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
