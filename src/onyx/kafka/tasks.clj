(ns onyx.kafka.tasks
  (:require [cheshire.core :as json]
            [schema.core :as s]
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

(def UserTaskMapKey
  (os/build-allowed-key-ns :kafka))

(def KafkaInputTaskMap
  (s/->Both [os/TaskMap
             {:kafka/topic s/Str
              :kafka/group-id s/Str
              :kafka/zookeeper s/Str
              :kafka/offset-reset (s/enum :smallest :largest)
              :kafka/force-reset? s/Bool
              :kafka/deserializer-fn os/NamespacedKeyword
              (s/optional-key :kafka/partition) s/Str
              (s/optional-key :kafka/chan-capacity) s/Num
              (s/optional-key :kafka/fetch-size) s/Num
              (s/optional-key :kafka/empty-read-back-off) s/Num
              (s/optional-key :kafka/commit-interval) s/Num
              (s/optional-key :kafka/wrap-with-metadata?) s/Bool
              UserTaskMapKey s/Any}]))

(s/defn ^:always-validate kafka-input
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kafka/read-messages
                             :onyx/type :input
                             :onyx/medium :kafka
                             :kafka/chan-capacity 1000
                             :kafka/fetch-size 307200
                             :kafka/empty-read-back-off 500
                             :kafka/commit-interval 2000
                             :kafka/wrap-with-metadata? false
                             :onyx/doc "Reads messages from a Kafka topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]}
    :schema {:task-map KafkaInputTaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    topic :- s/Str
    partition :- s/Str
    group-id :- s/Str
    zookeeper :- s/Str
    offset-reset :- (s/enum :smallest :largest)
    force-reset? :- s/Bool
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (kafka-input task-name (merge {:kafka/topic topic
                                  :kafka/partition partition
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
  (s/->Both [os/TaskMap
             {:kafka/topic s/Str
              :kafka/zookeeper s/Str
              :kafka/serializer-fn os/NamespacedKeyword
              :kafka/request-size s/Num
              UserTaskMapKey s/Any}]))

(s/defn ^:always-validate kafka-output
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kafka/write-messages
                             :onyx/type :output
                             :onyx/medium :kafka
                             :onyx/doc "Writes messages to a kafka topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}]}
    :schema {:task-map KafkaOutputTaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    topic :- s/Str
    zookeeper :- s/Str
    serializer-fn :- os/NamespacedKeyword
    request-size :- s/Num
    task-opts :- {s/Any s/Any}]
   (kafka-input task-name (merge {:kafka/topic topic
                                  :kafka/zookeeper zookeeper
                                  :kafka/serializer-fn serializer-fn
                                  :kafka/request-size request-size}
                                 task-opts))))
