(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!! close! timeout alts!! sliding-buffer]]
            [clj-kafka.producer :as kp]
            [clj-kafka.zk :as kzk]
            [clj-kafka.consumer.simple :as kc]
            [clj-kafka.core :as k]
            [cheshire.core :refer [parse-string]]
            [zookeeper :as zk]
            [onyx.peer.pipeline-extensions :as p-ext]
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
      x
      (read-from-bound consumer topic kpartition task-map))))

(defn highest-offset-to-commit [offsets]
  (->> (partition-all 2 1 offsets)
       (partition-by #(- (or (second %) (first %)) (first %)))
       (first)
       (last)
       (last)))

(defn commit-loop [group-id m topic kpartition commit-interval pending-commits]
  (try
    (loop []
      (Thread/sleep commit-interval)
      (when-let [offset (highest-offset-to-commit @pending-commits)]
        (log/info "Writing offset to ZooKeeper: " offset)
        (kzk/set-offset! m group-id topic kpartition offset)
        (swap! pending-commits (fn [coll] (remove (fn [k] (<= k offset)) coll))))
      (recur))
    (catch InterruptedException e
      (throw e))
    (catch Throwable e
      (fatal e))))

(defn kw->fn [kw]
  (try
    (let [user-ns (symbol (name (namespace kw)))
          user-fn (symbol (name kw))]
      (or (ns-resolve user-ns user-fn) (throw (Exception.))))
    (catch Throwable e
      (throw (ex-info "Could not resolve symbol on the classpath, did you require the file that contains this symbol?" {:symbol kw})))))

(defn reader-loop [m client-id group-id topic partitions kpartition task-map ch pending-commits]
  (try
    (loop []
      (try
        (let [brokers (get partitions (str kpartition))
              broker (get (id->broker m) (first brokers))
              consumer (kc/consumer (:host broker) (:port broker) client-id)
              offset (starting-offset m consumer topic kpartition group-id task-map)
              fetch-size (or (:kafka/fetch-size task-map) (:kafka/fetch-size defaults))
              messages (kc/messages consumer "onyx" topic kpartition offset fetch-size)
              empty-read-back-off (or (:kafka/empty-read-back-off task-map) (:kafka/empty-read-back-off defaults))
              commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
              commit-fut (future (commit-loop group-id m topic kpartition commit-interval pending-commits))
              deserializer-fn (kw->fn (:kafka/deserializer-fn task-map))]
          (log/info "Opening Kafka consumer" task-map)
          (log/info (str "Kafka consumer is starting at offset " offset))
          (try
            (loop [ms messages]
              (if (first ms)
                (>!! ch {:message (deserializer-fn (.value ^KafkaMessage (first ms)))
                         :offset (.offset ^int (first ms))})
                (Thread/sleep empty-read-back-off))
              (recur (rest ms)))
            (finally
             (future-cancel commit-fut))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e))))
    (catch InterruptedException e
      (throw e))))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id]} task-map
        kpartition (Integer/parseInt partition)
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        chan-capacity (or (:kafka/chan-capacity task-map) (:kafka/chan-capacity defaults))
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (kzk/partitions m topic)
        ch (chan chan-capacity)
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        reader-fut (future (reader-loop m client-id group-id topic partitions kpartition task-map ch pending-commits))]
    {:kafka/read-ch ch
     :kafka/reader-future reader-fut
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits}))

(defmethod p-ext/read-batch :kafka/read-messages
  [{:keys [kafka/read-ch kafka/pending-messages onyx.core/task-map] :as event}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            {:id (java.util.UUID/randomUUID)
                             :input :kafka
                             :message (:message result)
                             :offset (:offset result)})))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message :offset])))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :kafka/read-messages
  [{:keys [kafka/pending-messages kafka/pending-commits onyx.core/log onyx.core/task-id]} message-id]
  (when-let [offset (:offset (get @pending-messages message-id))]
    (swap! pending-commits conj offset))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :kafka/read-messages
  [{:keys [kafka/pending-messages kafka/read-ch onyx.core/log]} message-id]
  (>!! read-ch (get @pending-messages message-id))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? :kafka/read-messages
  [{:keys [kafka/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :kafka/read-messages
  [{:keys [kafka/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (map :message (vals x))) :done))))

(defn close-read-messages
  [{:keys [kafka/read-ch] :as pipeline} lifecycle]
  (future-cancel (:kafka/reader-future pipeline))
  (close! read-ch)
  {})

(defn inject-write-messages
  [{:keys [onyx.core/task-map] :as pipeline} lifecycle]
  (let [bl (kzk/broker-list (kzk/brokers {"zookeeper.connect" (:kafka/zookeeper task-map)}))
        config {"metadata.broker.list" bl
                "partitioner.class" (:kafka/partitioner-class task-map)}]
    {:kafka/config config
     :kafka/topic (:kafka/topic task-map)
     :kafka/serializer-fn (kw->fn (:kafka/serializer-fn task-map))
     :kafka/producer (kp/producer config)}))

(defmethod p-ext/write-batch :kafka/write-messages
  [{:keys [onyx.core/results kafka/producer kafka/topic kafka/serializer-fn]}]
  (let [messages (mapcat :leaves results)]
    (doseq [m (map :message messages)]
      (kp/send-message producer (kp/message topic (serializer-fn m)))))
  {})

(defmethod p-ext/seal-resource :kafka/write-messages
  [{:keys [kafka/producer kafka/topic kafka/serializer-fn]}]
  (kp/send-message producer (kp/message topic (serializer-fn :done)))
  {})

(def read-messages-calls
  {:lifecycle/before-task-start start-kafka-consumer
   :lifecycle/after-task-stop close-read-messages})

(def write-messages-calls
  {:lifecycle/before-task-start inject-write-messages})
