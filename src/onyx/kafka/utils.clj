(ns onyx.kafka.utils
  (:require [franzy.clients.consumer.client :as consumer]
            [franzy.clients.consumer.protocols :refer [poll! assign-partitions!]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [franzy.serialization.deserializers :refer [byte-array-deserializer]]
            [onyx.plugin.kafka :refer [id->broker]]))

(defn take-until-done
  "Reads from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-until-done zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (let [config {:bootstrap.servers (vals (id->broker zk-addr))
                 :group.id "onyx-consumer"
                 :auto.offset.reset :earliest
                 :enable.auto.commit false}
         key-deserializer (byte-array-deserializer)
         value-deserializer (byte-array-deserializer)
         c (consumer/make-consumer config key-deserializer value-deserializer)]
     (assign-partitions! c [{:topic topic :partition 0}])
     (loop [results []]
       (let [msgs (into [] (poll! c))
             segments
             (map
              (fn [m]
                {:key (when (:key m) (decompress-fn (:key m)))
                 :value (decompress-fn (:value m))
                 :partition (:partition m)})
              msgs)]
         (if (= :done (:value (last segments)))
           (into results (butlast segments))
           (recur (into results segments))))))))

(defn take-segments
  "Reads segments from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-segments zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (conj (mapv :value
               (take-until-done zk-addr topic decompress-fn opts))
         :done)))
