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
  (:import [kafka.message Message]))

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

(defn starting-offset [m consumer topic partition group-id task-map]
  (cond (= (:kafka/offset-reset task-map) :smallest)
        (kc/topic-offset consumer topic partition :earliest)
        (= (:kafka/offset-reset task-map) :largest)
        (kc/topic-offset consumer topic partition :latest)
        :else (kzk/committed-offset m group-id topic partition)))

(defn highest-offset-to-commit [offsets]
  (->> (partition-all 2 1 offsets)
       (partition-by #(- (or (second %) (first %)) (first %)))
       (first)
       (last)
       (last)))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id
                kafka/fetch-size kafka/chan-capacity]} task-map
        partition (Integer/parseInt partition)
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (kzk/partitions m topic)
        brokers (get partitions (str partition))
        broker (get (id->broker m) (first brokers))
        consumer (kc/consumer (:host broker) (:port broker) client-id)
        offset (starting-offset m consumer topic partition group-id task-map)
        messages (kc/messages consumer "onyx" topic partition offset fetch-size)
        ch (chan (sliding-buffer chan-capacity))
        pending-messages (atom {})
        pending-commits (atom (sorted-set))
        reader-fut (future
                     (try
                       (log/debug "Opening Kafka resource " task-map)
                       (loop [ms messages]
                         (if (first ms)
                           (>!! ch {:message (.payload ^Message (first ms))
                                    :offset (.offset ^int (first ms))})
                           (Thread/sleep (:kafka/empty-read-back-off task-map)))
                         (recur (rest ms)))
                       (catch InterruptedException e)
                       (catch Throwable e
                         (fatal e))))
        commit-fut (future
                     (try
                       (loop []
                         (Thread/sleep (:kafka/commit-interval task-map))
                         (let [offset (highest-offset-to-commit @pending-commits)]
                           (kzk/set-offset! m consumer topic partition offset)
                           (swap! pending-commits (fn [coll] (remove (fn [k] (<= k offset)) coll)))
                           (recur)))
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
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            {:id (java.util.UUID/randomUUID)
                             :input :kafka
                             :message (if result (read-string (String. (:message result) "UTF-8")))
                             :offset (:offset result)})))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message :offset])))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :kafka/read-messages
  [{:keys [kafka/pending-messages kafka/pending-commits onyx.core/log onyx.core/task-id]} message-id]
  (swap! pending-commits conj (:offset (get pending-messages message-id)))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :kafka/read-messages
  [{:keys [kafka/pending-messages kafka/read-ch onyx.core/log]} message-id]
  (let [msg (get @pending-messages message-id)]
    (if (= :done (:message msg))
      (>!! read-ch :done)
      (>!! read-ch (get @pending-messages message-id))))
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
