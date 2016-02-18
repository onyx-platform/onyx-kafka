(ns onyx.kafka.tasks.output-task
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [onyx.schema :as os]))


(defn serialize-message-json [segment]
  (.getBytes (json/generate-string segment)))

(defn serialize-message-edn [segment]
  (.getBytes segment))

(s/defschema KafkaOutputSchema
  {(s/required-key :kafka/topic) s/Str
   (s/required-key :kafka/zookeeper) s/Str
   (s/required-key :kafak/serializer-fn) os/NamespacedKeyword
   (s/optional-key :kafka/request-size) s/Num})

(s/defn output-task
  [task-name :- s/Keyword opts]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.kafka/write-messages
                            :onyx/type :output
                            :onyx/medium :kafka
                            :onyx/doc "Writes messages to a Kafka topic"}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls :onyx.plugin.kafka/write-messages-calls}]}
   :schema {:task-map (merge os/TaskMap KafkaOutputSchema)
            :lifecycles [os/Lifecycle]}})
