(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!! close! timeout alts!!]]
            [clojure.data.fressian :as fressian]
            [clj-kafka.consumer.zk :as zk]
            [clj-kafka.producer :as kp]
            [clj-kafka.core :as k]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [fatal] :as log]))

(defmethod l-ext/inject-lifecycle-resources :kafka/read-messages
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"zookeeper.connect" (:kafka/zookeeper task-map)
                "group.id" (:kafka/group-id task-map)
                "auto.offset.reset" (:kafka/offset-reset task-map)
                "auto.commit.enable" "true"}
        ch (chan (:kafka/chan-capacity task-map))]
    {:kafka/future (future
                     (try
                       (log/debug "Opening Kafka resource " config)
                       (k/with-resource [c (zk/consumer config)]
                         zk/shutdown
                         (loop [ms (zk/messages c (:kafka/topic task-map))]
                           (>!! ch (:value (first ms)))
                           (recur (rest ms))))
                       (catch InterruptedException e)
                       (catch Exception e
                         (fatal e)))
                     (log/debug "Stopping Kafka consumer and cleaning up"))
     :kafka/ch ch}))

(defmethod p-ext/read-batch [:input :kafka]
  [{:keys [kafka/ch onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)]
    {:onyx.core/batch
     (->> (range (:onyx/batch-size task-map))
          (map (fn [_]
                 {:input :kafka
                  :message (first (alts!! [ch (timeout ms)]))}))
          (filter (comp not nil? :message)))}))

(defmethod p-ext/decompress-batch [:input :kafka]
  [{:keys [onyx.core/batch] :as event}]
  {:onyx.core/decompressed (map (comp fressian/read :message) batch)})

(defmethod p-ext/strip-sentinel [:input :kafka]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/tail-batch? (= (last decompressed) :done)
   :onyx.core/requeue? false
   :onyx.core/decompressed (remove (partial = :done) decompressed)})

(defmethod p-ext/apply-fn [:input :kafka]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/results decompressed})

(defmethod l-ext/close-lifecycle-resources :kafka/read-messages
  [_ {:keys [kafka/ch] :as pipeline}]
  (future-cancel (:kafka/future pipeline))
  (close! ch)
  {})

(defmethod l-ext/inject-lifecycle-resources :kafka/write-messages
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"metadata.broker.list" (:kafka/brokers task-map)
                "serializer.class" (:kafka/serializer-class task-map)
                "partitioner.class" (:kafka/partitioner-class task-map)}]
    {:kafka/config config
     :kafka/topic (:kafka/topic task-map)
     :kafka/producer (kp/producer config)}))

(defmethod p-ext/apply-fn [:output :kafka]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/results decompressed})

(defmethod p-ext/compress-batch [:output :kafka]
  [{:keys [onyx.core/results]}]
  {:onyx.core/compressed (map #(.array (fressian/write %)) results)})

(defmethod p-ext/write-batch [:output :kafka]
  [{:keys [onyx.core/compressed kafka/producer kafka/topic]}]
  (doseq [c compressed]
    (kp/send-message producer (kp/message topic c)))
  {})

(defmethod p-ext/seal-resource [:output :kafka]
  [{:keys [kafka/producer kafka/topic]}]
  (kp/send-message producer (kp/message topic (.array (fressian/write :done))))
  {})

