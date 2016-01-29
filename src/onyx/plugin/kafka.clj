(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!! close! timeout alts!! sliding-buffer]]
            [clj-kafka.new.producer :as kp]
            [clj-kafka.zk :as kzk]
            [clj-kafka.consumer.simple :as kc]
            [clj-kafka.core :as k]
            [cheshire.core :refer [parse-string]]
            [zookeeper :as zk]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.types :as t]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.peer.operation :refer [kw->fn]]
            [taoensso.timbre :as log :refer [fatal info]])
  (:import [clj_kafka.core KafkaMessage]))

(def defaults
  {:kafka/fetch-size 307200
   :kafka/request-size 307200
   :kafka/chan-capacity 1000
   :kafka/empty-read-back-off 500
   :kafka/commit-interval 2000
   :kafka/wrap-with-metadata? false})

(defn log-id [replica-val job-id peer-id task-id]
  (get-in replica-val [:task-slot-ids job-id task-id peer-id]))

;; kafka operations

(defn id->broker [m]
  (k/with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (if-let [broker-ids (zk/children z "/brokers/ids")]
      (doall
       (into
        {}
        (map
         (fn [id]
           (let [s (String. ^bytes (:data (zk/data z (str "/brokers/ids/" id))))]
             {(Integer/parseInt id) (parse-string s true)}))
         broker-ids)))
      [])))

(defn read-from-bound [consumer topic kpartition task-map]
  (cond (= (:kafka/offset-reset task-map) :smallest)
        (kc/topic-offset consumer topic kpartition :earliest)
        (= (:kafka/offset-reset task-map) :largest)
        (kc/topic-offset consumer topic kpartition :latest)
        :else (throw (ex-info ":kafka/offset-reset must be either :smallest or :largest" {:task-map task-map}))))

(defn starting-offset [m consumer topic kpartition group-id task-map]
  (if (:kafka/force-reset? task-map)
    (read-from-bound consumer topic kpartition task-map)
    (if-let [x (kzk/committed-offset m group-id topic kpartition)]
      (inc x)
      (read-from-bound consumer topic kpartition task-map))))

(defn highest-offset-to-commit [offsets]
  (->> (sort offsets)
       (partition-all 2 1)
       (partition-by #(- (or (second %) (first %)) (first %)))
       (first)
       (last)
       (last)))

(defn commit-loop [group-id m topic kpartition commit-interval pending-commits]
  (try
    (loop []
      (Thread/sleep commit-interval)
      (when-let [offset (highest-offset-to-commit @pending-commits)]
        (kzk/set-offset! m group-id topic kpartition offset)
        (swap! pending-commits (fn [coll] (remove (fn [k] (<= k offset)) coll))))
      (recur))
    (catch InterruptedException e
      (throw e))
    (catch Throwable e
      (fatal e))))

(defn reader-loop [m client-id group-id topic static-partition partitions task-map 
                   replica job-id peer-id task-id ch pending-commits]
  (try
    (loop []
      (try
        (let [kpartition (if static-partition
                           (Integer/parseInt (str static-partition))
                           (log-id @replica job-id peer-id task-id)) 
              _ (log/info "Kafka task:" task-id "allocated to partition:" kpartition)
              brokers (get partitions (str kpartition))
              broker (get (id->broker m) (first brokers))
              consumer (kc/consumer (:host broker) (:port broker) client-id)
              offset (starting-offset m consumer topic kpartition group-id task-map)
              fetch-size (or (:kafka/fetch-size task-map) (:kafka/fetch-size defaults))
              empty-read-back-off (or (:kafka/empty-read-back-off task-map) (:kafka/empty-read-back-off defaults))
              commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
              commit-fut (future (commit-loop group-id m topic kpartition commit-interval pending-commits))
              deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))
              wrap-message? (or (:kafka/wrap-with-metadata? task-map) (:kafka/wrap-with-metadata? defaults))
              wrapper-fn (if wrap-message?
                           (fn [^KafkaMessage kafka-message value off] 
                             {:offset off 
                              :message value 
                              :topic topic 
                              ; There appears to be a bug in clj-kafka
                              ; so we will just supply the know information in the static case
                              :partition kpartition
                              :key (:key kafka-message)})
                           (fn [_ value _] value))]
          (log/info (str "Kafka consumer is starting at offset " offset))
          (try
            (loop [ms (kc/messages consumer "onyx" topic kpartition offset fetch-size)
                   head-offset offset]
              (if-not (seq ms)
                (let [_ (Thread/sleep empty-read-back-off)
                      fetched (kc/messages consumer "onyx" topic kpartition head-offset fetch-size)]
                  (recur fetched head-offset))
                (let [message ^KafkaMessage (first ms)
                      next-offset ^int (.offset message)
                      dm (deserializer-fn (.value message))
                      wrapped (wrapper-fn message dm next-offset)]
                  (>!! ch (assoc (t/input (random-uuid) wrapped)
                                 :offset next-offset))
                  (recur (rest ms) (inc next-offset)))))
            (finally
             (future-cancel commit-fut))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          ;; pass exception back to reader thread
          (>!! ch (t/input (random-uuid) e)))))
    (catch InterruptedException e
      (throw e))))

(defn check-num-peers-equals-partitions [{:keys [onyx/min-peers onyx/max-peers
                                                 kafka/partition] :as task-map} 
                                         n-partitions]
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
  [{:keys [onyx.core/task-map onyx.core/pipeline] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id]} task-map
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (:partitions pipeline)
        n-partitions (count partitions)
        _ (check-num-peers-equals-partitions task-map n-partitions)
        ch (:read-ch pipeline)
        pending-messages (:pending-messages pipeline)
        pending-commits (:pending-commits pipeline)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)
        reader-fut (future (reader-loop m client-id group-id topic partition partitions task-map 
                                        (:onyx.core/replica event) job-id peer-id task-id
                                        ch pending-commits))]
    {:kafka/read-ch ch
     :kafka/reader-future reader-fut
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits}))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defrecord KafkaReadMessages [max-pending batch-size batch-timeout partitions done-unsupported? 
                              pending-messages pending-commits drained? read-ch]
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
                              (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
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
                 (or (not (empty? @pending-messages))
                     (not (empty? batch))))
        (if done-unsupported? 
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
      (>!! read-ch (t/input (random-uuid) (:message msg)))
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
        m {"zookeeper.connect" (:kafka/zookeeper catalog-entry)}
        partitions (kzk/partitions m (:kafka/topic catalog-entry))
        done-unsupported? (and (> (count partitions) 1) 
                               (not (:kafka/partition catalog-entry)))
        ch (chan chan-capacity)
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        drained? (atom false)]
    (->KafkaReadMessages max-pending batch-size batch-timeout partitions done-unsupported?
                         pending-messages pending-commits drained? ch)))

(defn close-read-messages
  [{:keys [kafka/read-ch] :as pipeline} lifecycle]
  (future-cancel (:kafka/reader-future pipeline))
  (close! read-ch)
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

(defrecord KafkaWriteMessages [config topic producer serializer-fn]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results]}]
    (let [messages (mapcat :leaves (:tree results))
          send-futs (doall 
                      (map (fn [m]
                             (let [k-message (:message m)
                                   k-key (some-> m :key serializer-fn)
                                   p (some-> m :partition int)]
                               (assert k-message
                                       "Messages must be supplied in a map in form {:message :somevalue}, with optional :key and :partition keys.")
                               (kp/send producer (kp/record topic p k-key (serializer-fn k-message)))))
                           (map :message messages)))]
      (doall (map deref send-futs)))
    {})

  (seal-resource
    [_ {:keys [onyx.core/results]}]
    @(kp/send producer (kp/record topic (serializer-fn :done)))))

(defn write-messages [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        bl (kzk/broker-list (kzk/brokers {"zookeeper.connect" (:kafka/zookeeper task-map)}))
        ;; support some additional opts here
        request-size (or (get task-map :kafka/request-size) (get defaults :kafka/request-size))
        config {"bootstrap.servers" bl
                "key.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                "max.request.size" (str request-size)}
        topic (:kafka/topic task-map)
        producer (kp/producer config)
        serializer-fn (kw->fn (:kafka/serializer-fn task-map))]
    (->KafkaWriteMessages config topic producer serializer-fn)))

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
