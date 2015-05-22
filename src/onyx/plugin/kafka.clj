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

(defn read-from-bound [consumer topic partition task-map]
  (cond (= (:kafka/offset-reset task-map) :smallest)
        (kc/topic-offset consumer topic partition :earliest)
        (= (:kafka/offset-reset task-map) :largest)
        (kc/topic-offset consumer topic partition :latest)
        :else (throw (ex-info ":kafka/offset-reset must be either :smallest or :largest" {:task-map task-map}))))

(defn starting-offset [m consumer topic partition group-id task-map]
  (if (:kafka/force-reset? task-map)
    (read-from-bound consumer topic partition task-map)
    (if-let [x (kzk/committed-offset m group-id topic partition)]
      x
      (read-from-bound consumer topic partition task-map))))

(defn highest-offset-to-commit [offsets]
  (->> (partition-all 2 1 offsets)
       (partition-by #(- (or (second %) (first %)) (first %)))
       (first)
       (last)
       (last)))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id]} task-map
        fetch-size (or (:kafka/fetch-size task-map) (:kafka/fetch-size defaults))
        chan-capacity (or (:kafka/chan-capacity task-map) (:kafka/chan-capacity defaults))
        empty-read-back-off (or (:kafka/empty-read-back-off task-map) (:kafka/empty-read-back-off defaults))
        commit-interval (or (:kafka/commit-interval task-map) (:kafka/commit-interval defaults))
        partition (Integer/parseInt partition)
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (kzk/partitions m topic)
        brokers (get partitions (str partition))
        broker (get (id->broker m) (first brokers))
        consumer (kc/consumer (:host broker) (:port broker) client-id)
        offset (starting-offset m consumer topic partition group-id task-map)
        messages (kc/messages consumer "onyx" topic partition offset fetch-size)
        ch (chan chan-capacity)
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        reader-fut (future
                     (try
                       (log/debug "Opening Kafka consumer" task-map)
                       (log/debug (str "Kafka consumer is starting at offset " offset))
                       (loop [ms messages]
                         (if (first ms)
                           (>!! ch {:message (read-string (String. (.value ^KafkaMessage (first ms)) "UTF-8"))
                                    :offset (.offset ^int (first ms))})
                           (Thread/sleep empty-read-back-off))
                         (recur (rest ms)))
                       (catch InterruptedException e)
                       (catch Throwable e
                         (fatal e))))
        commit-fut (future
                     (try
                       (loop []
                         (Thread/sleep commit-interval)
                         (when-let [offset (highest-offset-to-commit @pending-commits)]
                           (kzk/set-offset! m consumer topic partition offset)
                           (swap! pending-commits (fn [coll] (remove (fn [k] (<= k offset)) coll))))
                         (recur))
                       (catch InterruptedException e)
                       (catch Throwable e
                         (fatal e))))]
    {:kafka/consumer consumer
     :kafka/messages messages
     :kafka/read-ch ch
     :kafka/reader-future reader-fut
     :kafka/commit-future commit-fut
     :kafka/pending-messages pending-messages
     :kafka/pending-commits pending-commits}))

(defmethod p-ext/read-batch :kafka/read-messages
  [{:keys [kafka/read-ch kafka/pending-messages onyx.core/task-map] :as event}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 500)
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
  (when-let [offset (:offset (get pending-messages message-id))]
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
  (future-cancel (:kafka/commit-future pipeline))
  (close! read-ch)
  {})

(defn inject-write-messages
  [{:keys [onyx.core/task-map] :as pipeline} lifecycle]
  (let [config {"metadata.broker.list" (:kafka/brokers task-map)
                "serializer.class" (:kafka/serializer-class task-map)
                "partitioner.class" (:kafka/partitioner-class task-map)}]
    {:kafka/config config
     :kafka/topic (:kafka/topic task-map)
     :kafka/producer (kp/producer config)}))

(defmethod p-ext/write-batch :kafka/write-messages
  [{:keys [onyx.core/results kafka/producer kafka/topic]}]
  (let [messages (mapcat :leaves results)]
    (doseq [m (map :message messages)]
      (kp/send-message producer (kp/message topic (.getBytes (pr-str m))))))
  {})

(defmethod p-ext/seal-resource :kafka/write-messages
  [{:keys [kafka/producer kafka/topic]}]
  (kp/send-message producer (kp/message topic (.getBytes (pr-str :done))))
  {})

(def read-messages-calls
  {:lifecycle/before-task-start start-kafka-consumer
   :lifecycle/after-task-stop close-read-messages})

(def write-messages-calls
  {:lifecycle/before-task-start inject-write-messages})
