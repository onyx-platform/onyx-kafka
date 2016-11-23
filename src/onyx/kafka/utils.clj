(ns onyx.kafka.utils
  (:require [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :refer [poll! assign-partitions!]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [onyx.plugin.kafka :refer [id->broker]]
            [taoensso.timbre :as log]
            [aero.core :refer [read-config]]
            [clojure.core.async :as async]))

(defmacro ^:private timeout
  {:indent 1}
  [ms & body]
  `(let [ch# (async/thread ~@body)]
     (first (async/alts!! [ch# (async/timeout ~ms)]))))

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

(defn take-until-done
  "Reads from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-until-done zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (log/info {:msg "Taking until done..." :topic topic})
   (timeout (or (:timeout opts) 5000)
     (let [c (make-consumer zk-addr)]
       (assign-partitions! c [{:topic topic :partition 0}])
       (loop [results []]
         (let [msgs (into [] (poll! c {:poll-timeout-ms 500}))
               segments (map #(consumer-record->message decompress-fn %) msgs)]
           (if (= :done (:value (last segments)))
             (into results (butlast segments))
             (recur (into results segments)))))))))

(defn take-now
  "Reads whatever it can from a topic on the assumption that we've distributed
  work across multiple topics and another topic contained :done."
  [zk-addr topic decompress-fn]
  (log/info {:msg "Taking now..." :topic topic})
  (let [c (make-consumer zk-addr)]
    (assign-partitions! c [{:topic topic :partition 0}])
    (mapv #(consumer-record->message decompress-fn %) (poll! c {:poll-timeout-ms 5000}))))

(defn take-segments
  "Reads segments from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-segments zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (conj (mapv :value
               (take-until-done zk-addr topic decompress-fn opts))
         :done)))
