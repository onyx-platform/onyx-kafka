(ns onyx.plugin.kafka
  (:require [clojure.core.async :as a :refer [chan >!! <!! close! timeout alts!! sliding-buffer]]
            [franzy.admin.zookeeper.client :as k-admin]
            [franzy.admin.cluster :as k-cluster]
            [franzy.admin.partitions :as k-partitions]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [franzy.clients.producer.client :as producer]
            [franzy.clients.producer.protocols :refer [send-async! send-sync!]]
            [franzy.clients.producer.types :refer [make-producer-record]]
            [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :refer [assign-partitions! commit-offsets-sync!
                                                       poll! seek-to-offset!] :as cp]
            [taoensso.timbre :as log :refer [fatal info]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.peer.function :as function]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.extensions :as extensions]
            [onyx.types :as t]
            [onyx.api])
  (:import [franzy.clients.producer.types ProducerRecord]))

(def defaults
  {:kafka/fetch-size 307200
   :kafka/request-size 307200
   :kafka/chan-capacity 1000
   :kafka/empty-read-back-off 500
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

(defn checkpoint-or-beginning [log consumer topic kpartition group-id]
  (let [k (checkpoint-name group-id topic kpartition)]
    (try
      (let [offset (inc (:offset (extensions/read-chunk log :chunk k)))]
        (seek-to-offset! consumer {:topic topic :partition kpartition} offset))
      (catch org.apache.zookeeper.KeeperException$NoNodeException nne
        (cp/seek-to-beginning-offset! consumer [{:topic topic :partition kpartition}])))))

(defn seek-offset! [log consumer group-id topic kpartition task-map]
  (if (:kafka/force-reset? task-map)
    (let [policy (:kafka/offset-reset task-map)]
      (cond (= policy :smallest)
            (cp/seek-to-beginning-offset! consumer [{:topic topic :partition kpartition}])

            (= policy :largest)
            (cp/seek-to-end-offset! consumer [{:topic topic :partition kpartition}])

            :else
            (throw (ex-info "Tried to seek to unknown policy" {:policy policy}))))
    (checkpoint-or-beginning log consumer topic kpartition group-id)))

(defn log-id [replica-val job-id peer-id task-id]
  (get-in replica-val [:task-slot-ids job-id task-id peer-id]))

;; kafka operations

(defn id->broker [zk-addr]
  (let [config (k-admin/make-zk-utils {:servers zk-addr} false)]
    (reduce
     (fn [result {:keys [id endpoints]}]
       (assoc
        result
        id
        (str (get-in endpoints [:plaintext :host])
             ":"
             (get-in endpoints [:plaintext :port]))))
     {}
     (k-cluster/all-brokers config))))

(defn find-brokers [zk-addr]
  (let [results (vals (id->broker zk-addr))]
    (if (seq results)
      results
      (throw (ex-info "Could not locate any Kafka brokers to connect to."
                      {:zk-addr zk-addr})))))

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

(defn reader-loop [client-id group-id topic static-partition partitions task-map 
                   replica job-id peer-id task-id ch pending-commits log]
  (try
    (loop []
      (try
        (let [kpartition (if static-partition
                           (Integer/parseInt (str static-partition))
                           (log-id @replica job-id peer-id task-id))
              consumer-config {:bootstrap.servers (find-brokers (:kafka/zookeeper task-map))
                               :group.id group-id
                               :enable.auto.commit false
                               :auto.offset.reset (get-offset-reset task-map (:kafka/offset-reset task-map))}

              key-deserializer (byte-array-deserializer)
              value-deserializer (byte-array-deserializer)

              consumer (consumer/make-consumer consumer-config key-deserializer value-deserializer)
              _ (assign-partitions! consumer [{:topic topic :partition kpartition}])

              _ (seek-offset! log consumer group-id topic kpartition task-map)
              offset (cp/next-offset consumer {:topic topic :partition kpartition})

              empty-read-back-off (or (:kafka/empty-read-back-off task-map) (:kafka/empty-read-back-off defaults))
              commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
              commit-fut (future (commit-loop log group-id topic kpartition commit-interval pending-commits))
              deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))
              wrap-message? (or (:kafka/wrap-with-metadata? task-map) (:kafka/wrap-with-metadata? defaults))
              wrapper-fn (if wrap-message?
                           (fn [kafka-message value off] 
                             {:offset off 
                              :message value 
                              :topic (:topic kafka-message)
                              :partition (:partition kafka-message)
                              :key (:key kafka-message)})
                           (fn [_ value _] value))]
          _ (log/info (str "Kafka task: " task-id " allocated to partition: " kpartition ", starting at offset: " offset))
          (try
            (loop [msgs (seq (poll! consumer))]
              (when-not (Thread/interrupted)
                (if-not msgs
                  (Thread/sleep empty-read-back-off)
                  (doseq [message msgs]
                    (let [next-offset (:offset message)
                          dm (deserializer-fn (:value message))
                          wrapped (wrapper-fn message dm next-offset)]
                      (>!! ch (assoc (t/input (random-uuid) wrapped) 
                                     :offset next-offset)))))
                (recur (seq (poll! consumer)))))
            (finally
              (future-cancel commit-fut))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          ;; pass exception back to reader thread
          (>!! ch (t/input (random-uuid) e)))))
    (catch InterruptedException e
      (throw e))))

(defn check-num-peers-equals-partitions 
  [{:keys [onyx/min-peers onyx/max-peers kafka/partition] :as task-map} n-partitions]
  (let [fixed-partition? (and partition (= 1 max-peers))
        all-partitions-covered? (= n-partitions min-peers max-peers)
        one-partition? (= 1 n-partitions max-peers)] 
    (when-not (or fixed-partition? all-partitions-covered? one-partition?)
      (let [e (ex-info ":onyx/min-peers must equal :onyx/max-peers and the number of kafka partitions" 
                       {:n-partitions n-partitions 
                        :min-peers min-peers
                        :max-peers max-peers
                        :task-map task-map})] 
        (log/error e)
        (throw e)))))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map onyx.core/pipeline onyx.core/log] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id]} task-map
        brokers (find-brokers (:kafka/zookeeper task-map))
        consumer-config {:bootstrap.servers brokers
                         :group.id group-id}
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        client-id "onyx"
        zk-utils (k-admin/make-zk-utils {:servers (:kafka/zookeeper task-map)} false)
        partitions (first (vals (k-partitions/partitions-for zk-utils [topic])))
        n-partitions (count partitions)
        _ (check-num-peers-equals-partitions task-map n-partitions)
        ch (:read-ch pipeline)
        retry-ch (:retry-ch pipeline)
        pending-messages (:pending-messages pipeline)
        pending-commits (:pending-commits pipeline)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)
        reader-fut (future (reader-loop client-id group-id topic partition partitions task-map 
                                        (:onyx.core/replica event) job-id peer-id task-id
                                        ch pending-commits log))
        done-unsupported? (and (> (count partitions) 1)
                               (not (:kafka/partition task-map)))]
    {:kafka/read-ch ch
     :kafka/retry-ch ch
     :kafka/reader-future reader-fut
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits
     :kafka/done-unsupported? done-unsupported?}))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defrecord KafkaReadMessages
    [max-pending batch-size batch-timeout pending-messages
     pending-commits drained? read-ch retry-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (if (zero? max-segments) 
                  (<!! timeout-ch)
                  (->> (range max-segments)
                       (map (fn [_]
                              (let [result (first (alts!! [retry-ch read-ch timeout-ch] :priority true))]
                                (if (= (:message result) :done)
                                  (t/input (random-uuid) :done)
                                  result))))
                       (filter :message)))]
      (doseq [m batch]
        (when (instance? java.lang.Throwable (:message m))
          (throw (:message m)))

        (swap! pending-messages assoc (:id m) m))
      (when (and (all-done? (vals @pending-messages))
                 (all-done? batch)
                 (zero? (count (.buf read-ch)))
                 (zero? (count (.buf retry-ch)))
                 (or (not (empty? @pending-messages))
                     (not (empty? batch))))
        (if (:kafka/done-unsupported? event)
          (throw (UnsupportedOperationException. ":done is not supported for auto assigned kafka partitions. 
                                                 (:kafka/partition must be supplied)"))
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
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (arg-or-default :onyx/max-pending catalog-entry)
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (arg-or-default :onyx/batch-timeout catalog-entry)
        chan-capacity (or (:kafka/chan-capacity catalog-entry) (:kafka/chan-capacity defaults))
        ch (chan chan-capacity)
        retry-ch (chan (* 2 max-pending))
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        drained? (atom false)]
    (->KafkaReadMessages
     max-pending batch-size batch-timeout
     pending-messages pending-commits drained? ch retry-ch)))

(defn close-read-messages
  [{:keys [kafka/read-ch kafka/retry-ch] :as pipeline} lifecycle]
  (future-cancel (:kafka/reader-future pipeline))
  (close! read-ch)
  ;; Drain read buffer to be safe, otherwise reader channel will stay blocked on put
  (while (a/poll! read-ch))
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
  [{:keys [serializer-fn topic]} m]
  (let [message (:message m)
        k (some-> m :key serializer-fn)
        p (some-> m :partition int)
        message-topic (get m :topic topic)]
    (cond
      (nil? message)
      (throw (ex-info "Payload is missing required :message!"
                      {:payload m}))
      (nil? message-topic)
      (throw (ex-info
              (str "Unable to write message payload to Kafka! "
                   "Both :kafka/topic, and :topic in message payload "
                   "are missing!")
              {:payload m}))
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
            (map :message)
            (map (partial message->producer-record this))
            (map (partial send-async! producer))
            (map deref)))
      {}))

  (seal-resource
    [_ {:keys [onyx.core/results]}]
    (if (:kafka/no-seal? task-map)
      {}
      (send-sync! producer (ProducerRecord. topic nil nil (serializer-fn :done))))))

(defn write-messages [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        request-size (or (get task-map :kafka/request-size) (get defaults :kafka/request-size))
        config {:bootstrap.servers (vals (id->broker (:kafka/zookeeper task-map)))
                :max.request.size request-size}
        topic (:kafka/topic task-map)
        key-serializer (byte-array-serializer)
        value-serializer (byte-array-serializer)
        producer (producer/make-producer config key-serializer value-serializer)
        serializer-fn (kw->fn (:kafka/serializer-fn task-map))]
    (->KafkaWriteMessages task-map config topic producer serializer-fn)))

(defn read-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def read-messages-calls
  {:lifecycle/before-task-start start-kafka-consumer
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-messages})

(defn write-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def write-messages-calls
  {:lifecycle/before-task-start inject-write-messages
   :lifecycle/handle-exception write-handle-exception
   :lifecycle/after-task-stop close-write-resources})
