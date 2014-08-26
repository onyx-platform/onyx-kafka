(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [clj-kafka.consumer.zk :as zk]
            [clj-kafka.producer :as kp]
            [clj-kafka.core :as k]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :refer [fatal]]))

(defmethod l-ext/inject-lifecycle-resources
  :kafka/read-messages
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"zookeeper.connect" (:kafka/zookeeper task-map)
                "group.id" (:kafka/group-id task-map)
                "auto.offset.reset" (:kafka/offset-reset task-map)
                "auto.commit.enable" "true"}
        ch (chan (:kafka/chan-capacity task-map))]
    {:kafka/future (future
                     (try
                       (k/with-resource [c (zk/consumer config)]
                         zk/shutdown
                         (loop [ms (zk/messages c (:kafka/topic task-map))]
                           (>!! ch (:value (first ms)))
                           (recur (rest ms))))
                       (catch Exception e
                         (fatal e))))
     :kafka/ch ch}))

(defmethod p-ext/read-batch [:input :kafka]
  [{:keys [kafka/ch onyx.core/task-map] :as event}]
  {:onyx.core/batch (doall (map (fn [_] (<!! ch))
                                (range (:onyx/batch-size task-map))))})

(defmethod p-ext/decompress-batch [:input :kafka]
  [{:keys [onyx.core/batch]}]
  (let [x (map #(String. (:value %) "UTF-8") batch)]
    (prn "->>>" x)
    {:onyx.core/decompressed x}))

(defmethod p-ext/apply-fn [:input :kafka]
  [_]
  {})
