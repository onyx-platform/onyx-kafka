(ns onyx.tasks.kafka
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [onyx.job :refer [add-task]]
            [onyx.schema :as os])
  (:import [com.fasterxml.jackson.core JsonGenerationException]))

;;;; Reader task
(defn deserialize-message-json [^bytes bs]
  (try
    (json/parse-string (String. bs "UTF-8"))
    (catch Exception e
      {:error e})))

(defn deserialize-message-edn [^bytes bs]
  (try
    (read-string (String. bs "UTF-8"))
    (catch Exception e
      {:error e})))

(def KafkaInputTaskMap
  {:kafka/topic s/Str
   :kafka/offset-reset (s/enum :earliest :latest)
   :kafka/deserializer-fn os/NamespacedKeyword
   (s/optional-key :kafka/bootstrap-servers) [s/Str]
   (s/optional-key :kafka/zookeeper) s/Str
   (s/optional-key :kafka/deserializer) s/Str
   (s/optional-key :kafka/key-deserializer) s/Str
   (s/optional-key :kafka/key-deserializer-fn) os/NamespacedKeyword
   (s/optional-key :kafka/group-id) s/Str
   (s/optional-key :kafka/consumer-opts) {s/Any s/Any}
   (s/optional-key :kafka/start-offsets) {s/Int s/Int}
   (s/optional-key :kafka/receive-buffer-bytes) s/Int
   (s/optional-key :kafka/partition) (s/cond-pre s/Int s/Str)
   (s/optional-key :kafka/wrap-with-metadata?) s/Bool
   (s/optional-key :kafka/target-offsets) {s/Int s/Int}
   (os/restricted-ns :kafka) s/Any})

(s/defn ^:always-validate consumer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kafka/read-messages
                             :onyx/type :input
                             :onyx/medium :kafka
                             :kafka/receive-buffer-bytes 65536
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
    offset-reset :- (s/enum :earliest :latest)
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (consumer task-name (merge {:kafka/topic topic
                               :kafka/group-id group-id
                               :kafka/zookeeper zookeeper
                               :kafka/offset-reset offset-reset
                               :kafka/deserializer-fn deserializer-fn}
                              task-opts))))

;;;; Writer task
(defn serialize-message-json [segment]
  (try
    (.getBytes (json/generate-string segment))
    (catch JsonGenerationException e
      (throw (ex-info (format "Could not serialize segment: %s" segment)
                      {:recoverable? false
                       :segment segment
                       :cause e})))))

(defn serialize-message-edn [segment]
  (.getBytes (pr-str segment)))

(def KafkaOutputTaskMap
  {(s/optional-key :kafka/topic) s/Str
   :kafka/serializer-fn os/NamespacedKeyword
   (s/optional-key :kafka/bootstrap-servers) [s/Str]
   (s/optional-key :kafka/zookeeper) s/Str
   (s/optional-key :kafka/serializer) s/Str
   (s/optional-key :kafka/key-serializer) s/Str
   (s/optional-key :kafka/key-serializer-fn) os/NamespacedKeyword
   (s/optional-key :kafka/request-size) s/Num
   (s/optional-key :kafka/partition) (s/cond-pre s/Int s/Str)
   (s/optional-key :kafka/no-seal?) s/Bool
   (s/optional-key :kafka/producer-opts) {s/Any s/Any}
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
