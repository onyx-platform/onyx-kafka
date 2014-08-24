(ns onyx.plugin.kafka
  (:require [clj-kafka.consumer.zk :as zk]
            [clj-kafka.consumer.simple :refer [messages]]
            [clj-kafka.core :as kafka]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]))

(defmethod l-ext/inject-lifecycle-resources
  :kafka/read-messages
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"zookeeper.connect" (:kafka/zookeeper task-map)
                "group.id" (:kafka/group-id task-map)
                "auto.offset.reset" (:kafka/offset-reset task-map)}
        consumer (zk/consumer config)]
    {:kafka/consumer consumer
     :kafka/messages (messages consumer
                               (:kafka/topic task-map)
                               (:kafka/partition task-map)
                               (:kafka/offset task-map)
                               (:kafka/fetch-size task-map))}))

(defmethod p-ext/read-batch [:input :kafka]
  [{:keys [onyx.core/task-map] :as event}]
  {:onyx.core/batch (take (:onyx/batch-size task-map) (:kafka/messages event))})


(defmethod p-ext/apply-fn [:input :kafka]
  [_]
  {})

(def catalog
  [{:onyx/name :read-messages
    :onyx/ident :kafka/read-messages
    :onyx/type :input
    :onyx/medium :kafka
    :onyx/consumption :concurrent
    :kafka/topic "topic-name"
    :kafka/partition "partition-name"
    :kafka/offset 12345
    :kafka/fetch-size 1024
    :kafka/zookeeper "127.0.0.1:2181"
    :kafka/group-id "group-id"
    :kafka/offset-reset "-1"
    :onyx/batch-size 1024
    :onyx/doc "Reads messages from a Kafka topic"}]

)