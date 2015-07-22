(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!! close! timeout alts!! sliding-buffer]]
            [clj-kafka.producer :as kp]
            [clj-kafka.zk :as kzk]
            [clj-kafka.consumer.simple :as kc]
            [clj-kafka.core :as k]
            [cheshire.core :refer [parse-string]]
            [zookeeper :as zk]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.types :as t]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.peer.operation :refer [kw->fn]]
            [taoensso.timbre :as log :refer [fatal]])
  (:import [clj_kafka.core KafkaMessage]))

(def defaults
  {:kafka/fetch-size 307200
   :kafka/chan-capacity 1000
   :kafka/empty-read-back-off 500
   :kafka/commit-interval 2000})

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

(defn reader-loop [m client-id group-id topic partitions kpartition task-map ch pending-commits]
  (try
    (loop []
      (try
        (let [brokers (get partitions (str kpartition))
              broker (get (id->broker m) (first brokers))
              consumer (kc/consumer (:host broker) (:port broker) client-id)
              offset (starting-offset m consumer topic kpartition group-id task-map)
              fetch-size (or (:kafka/fetch-size task-map) (:kafka/fetch-size defaults))
              empty-read-back-off (or (:kafka/empty-read-back-off task-map) (:kafka/empty-read-back-off defaults))
              commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
              commit-fut (future (commit-loop group-id m topic kpartition commit-interval pending-commits))
              deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))]
          (log/info "Opening Kafka consumer" task-map)
          (log/info (str "Kafka consumer is starting at offset " offset))
          (try
            (loop [ms (kc/messages consumer "onyx" topic kpartition offset fetch-size)
                   head-offset offset]
              (if-not (seq ms)
                (let [fetched (kc/messages consumer "onyx" topic kpartition head-offset fetch-size)]
                  (Thread/sleep empty-read-back-off)
                  (recur fetched head-offset))
                (let [next-offset (.offset ^int (first ms))]
                  (>!! ch (assoc (t/input (java.util.UUID/randomUUID)
                                          (deserializer-fn (.value ^KafkaMessage (first ms))))
                                 :offset next-offset))
                  (recur (rest ms) (inc next-offset)))))
            (finally
             (future-cancel commit-fut))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e))))
    (catch InterruptedException e
      (throw e))))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map onyx.core/pipeline] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id]} task-map
        kpartition (Integer/parseInt partition)
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (kzk/partitions m topic)
        ch (:read-ch pipeline) 
        pending-messages (:pending-messages pipeline)
        pending-commits (:pending-commits pipeline)
        reader-fut (future (reader-loop m client-id group-id topic partitions kpartition task-map ch pending-commits))]
    {:kafka/read-ch ch
     :kafka/reader-future reader-fut
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits}))

(defrecord KafkaReadMessages [max-pending batch-size batch-timeout pending-messages 
                              pending-commits drained? read-ch]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (->> (range max-segments)
                     (map (fn [_]
                            (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                              (if (= (:message result) :done)
                                (t/input (java.util.UUID/randomUUID) :done)
                                result))))
                     (filter :message))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      (when (and (= 1 (count @pending-messages))
                 (= (count batch) 1)
                 (= (:message (first batch)) :done))
        (reset! drained? true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-message [_ _ message-id]
    (when-let [offset (:offset (get @pending-messages message-id))]
      (swap! pending-commits conj offset))
    (swap! pending-messages dissoc message-id))

  (retry-message 
    [_ _ message-id]
    (when-let [msg (get @pending-messages message-id)]
      (swap! pending-messages dissoc message-id)
      (>!! read-ch (t/input (java.util.UUID/randomUUID)
                            (:message msg)))))

  (pending?
    [_ _ message-id]
    (get @pending-messages message-id))

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
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        drained? (atom false)]
    (->KafkaReadMessages max-pending batch-size batch-timeout 
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

(defrecord KafkaWriteMessages [config topic producer serializer-fn]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ {:keys [onyx.core/results kafka/topic kafka/producer kafka/serializer-fn]}]
    (let [messages (mapcat :leaves (:tree results))]
      (doseq [m (map :message messages)]
        (kp/send-message producer (kp/message topic (serializer-fn m)))))
    {})

  (seal-resource 
    [_ {:keys [onyx.core/results]}]
    (kp/send-message producer (kp/message topic (serializer-fn :done)))))

(defn write-messages [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        bl (kzk/broker-list (kzk/brokers {"zookeeper.connect" (:kafka/zookeeper task-map)}))
        config {"metadata.broker.list" bl
                "partitioner.class" (:kafka/partitioner-class task-map)}
        topic (:kafka/topic task-map)
        producer (kp/producer config)
        serializer-fn (kw->fn (:kafka/serializer-fn task-map))]
    (->KafkaWriteMessages config topic producer serializer-fn)))

(def read-messages-calls
  {:lifecycle/before-task-start start-kafka-consumer
   :lifecycle/after-task-stop close-read-messages})

(def write-messages-calls
  {:lifecycle/before-task-start inject-write-messages})
