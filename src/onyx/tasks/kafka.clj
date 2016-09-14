(ns onyx.tasks.kafka
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [onyx.job :refer [add-task]]
            [onyx.schema :as os]))

;;;; Reader task
(defn deserialize-message-json [bytes]
  (try
    (json/parse-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(defn deserialize-message-edn [bytes]
  (try
    (read-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(def KafkaInputTaskMap
  {:kafka/topic s/Str
   :kafka/group-id s/Str
   :kafka/zookeeper s/Str
   :kafka/offset-reset (s/enum :smallest :largest)
   :kafka/force-reset? s/Bool
   :kafka/deserializer-fn os/NamespacedKeyword
   (s/optional-key :kafka/receive-buffer-bytes) s/Int
   (s/optional-key :kafka/partition) s/Str
   (s/optional-key :kafka/commit-interval) s/Num
   (s/optional-key :kafka/wrap-with-metadata?) s/Bool
   (os/restricted-ns :kafka) s/Any})

(s/defn ^:always-validate consumer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kafka/read-messages
                             :onyx/type :input
                             :onyx/medium :kafka
                             :kafka/receive-buffer-bytes 65536
                             :kafka/commit-interval 2000
                             :kafka/wrap-with-metadata? false
                             :onyx/doc "Reads messages from a Kafka topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]}
    :schema {:task-map KafkaInputTaskMap}})
  ([task-name :- s/Keyword
    topic :- s/Str
    group-id :- s/Str
    zookeeper :- s/Str
    offset-reset :- (s/enum :smallest :largest)
    force-reset? :- s/Bool
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (consumer task-name (merge {:kafka/topic topic
                               :kafka/group-id group-id
                               :kafka/zookeeper zookeeper
                               :kafka/offset-reset offset-reset
                               :kafka/force-reset? force-reset?
                               :kafka/deserializer-fn deserializer-fn}
                              task-opts))))

;;;; Writer task
(defn serialize-message-json [segment]
  (.getBytes (json/generate-string segment)))

(defn serialize-message-edn [segment]
  (.getBytes (pr-str segment)))

(def KafkaOutputTaskMap
  {(s/optional-key :kafka/topic) s/Str
   :kafka/zookeeper s/Str
   :kafka/serializer-fn os/NamespacedKeyword
   :kafka/request-size s/Num
   (s/optional-key :kafka/no-seal?) s/Bool
   (os/restricted-ns :kafka) s/Any})

(s/defn ^:always-validate producer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kafka/write-messages
                             :onyx/type :output
                             :onyx/medium :kafka
                             :onyx/doc "Writes messages to a kafka topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}]}
    :schema {:task-map KafkaOutputTaskMap}})
  ([task-name :- s/Keyword
    topic :- s/Str
    zookeeper :- s/Str
    serializer-fn :- os/NamespacedKeyword
    request-size :- s/Num
    task-opts :- {s/Any s/Any}]
   (producer task-name (merge {:kafka/topic topic
                               :kafka/zookeeper zookeeper
                               :kafka/serializer-fn serializer-fn
                               :kafka/request-size request-size}
                              task-opts))))
