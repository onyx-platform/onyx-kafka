(ns onyx.kafka.utils
  (:require [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :refer [poll! assign-partitions!]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [onyx.plugin.kafka :refer [id->broker]]
            [taoensso.timbre :as log]
            [aero.core :refer [read-config]]
            [clojure.core.async :as async]))

(defn- make-consumer
  [zk-addr]
  (consumer/make-consumer
   {:bootstrap.servers (vals (id->broker zk-addr))
    :group.id "onyx-consumer"
    :auto.offset.reset :earliest
    :receive.buffer.bytes 65536
    :enable.auto.commit false}
   (byte-array-deserializer)
   (byte-array-deserializer)))

(defn- consumer-record->message
  [decompress-fn m]
  {:key (some-> m :key decompress-fn)
   :partition (:partition m)
   :topic (:topic m)
   :value (-> m :value decompress-fn)})

(defn take-now
  "Reads whatever it can from a topic on the assumption that we've distributed
  work across multiple topics and another topic contained :done."
  ([zk-addr topic decompress-fn]
   (take-now zk-addr topic decompress-fn 5000))
  ([zk-addr topic decompress-fn timeout]
   (log/info {:msg "Taking now..." :topic topic})
   (let [c (make-consumer zk-addr)]
     (assign-partitions! c [{:topic topic :partition 0}])
     (mapv #(consumer-record->message decompress-fn %) (poll! c {:poll-timeout-ms timeout})))))
