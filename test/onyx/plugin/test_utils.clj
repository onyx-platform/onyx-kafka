(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [<! go-loop]]
            [com.stuartsierra.component :as component]
            [onyx.kafka.helpers :as h]
            [aero.core]
            [schema.core :as s]
            [taoensso.timbre :as log]))

(defn read-config []
  (aero.core/read-config (clojure.java.io/resource "config.edn") 
                         {:profile (if (System/getenv "CIRCLECI")
                                     :ci
                                     :test)}))

;; Set the log level, otherwise Kafka emits a huge amount
;; of messages.
(.setLevel (org.apache.log4j.LogManager/getRootLogger) org.apache.log4j.Level/WARN)

(defn write-data
  "Starts a Kafka in-memory instance, preloading a topic with xs.
   If xs is a channel, will load the topic with items off the channel."
  ([topic zookeeper bootstrap-servers xs]
   (let [config {"bootstrap.servers" bootstrap-servers
                 "key.serializer" (h/byte-array-serializer-name)
                 "value.serializer" (h/byte-array-serializer-name)}
         prod (h/build-producer config)]
     (if (sequential? xs)
       (do
         (doseq [x xs]
           (h/send-sync! prod topic (int 0) nil (.getBytes (pr-str x))))
         (.close prod))
       (go-loop [itm (<! xs)]
         (if itm
           (do (h/send-sync! prod topic (int 0) nil (.getBytes (pr-str itm)))
               (recur (<! xs)))
           (.close prod)))))))
