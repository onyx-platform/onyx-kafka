(ns onyx.kafka.utils
  (:require [clj-kafka.consumer.zk :as zkconsumer]
            [taoensso.timbre :refer [info] :as timbre]
            [clj-kafka.core :as zkcore]))

(defn take-until-done
  "Reads from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-until-done zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (let [fetch-size (get opts :kafka/fetch-size 307200)
         kafka-config {"zookeeper.connect" zk-addr
                       "group.id" "onyx-consumer"
                       "auto.offset.reset" "smallest"
                       "auto.commit.enable" "false"
                       "fetch.message.max.bytes" (str fetch-size)}]
     (zkcore/with-resource [c (zkconsumer/consumer kafka-config)]
       zkconsumer/shutdown
       (->> (zkconsumer/messages c topic)
            (map (fn [msg] (-> msg 
                               (update :key #(if % 
                                               (decompress-fn %)))
                               (update :value decompress-fn))))
            (take-while (fn [v] (not= :done (:value v))))
            vec)))))


(defn take-segments
  "Reads segments from a topic until a :done is reached."
  ([zk-addr topic decompress-fn] (take-segments zk-addr topic decompress-fn {}))
  ([zk-addr topic decompress-fn opts]
   (conj (mapv :value 
               (take-until-done zk-addr topic decompress-fn opts))
         :done)))
