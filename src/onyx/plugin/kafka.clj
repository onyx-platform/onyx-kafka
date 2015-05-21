(ns onyx.plugin.kafka
  (:require [clojure.core.async :refer [chan >!! <!! close! timeout alts!!]]
            [clj-kafka.producer :as kp]
            [clj-kafka.zk :as kzk]
            [clj-kafka.consumer.simple :as kc]
            [clj-kafka.core :as k]
            [cheshire.core :refer [parse-string]]
            [zookeeper :as zk]
            [onyx.peer.pipeline-extensions :as p-ext]
            [taoensso.timbre :as log :refer [fatal]]))

(defn id->broker [m]
  (k/with-resource [z (zk/connect (get m "zookeeper.connect"))]
    zk/close
    (if-let [broker-ids (zk/children z "/brokers/ids")]
      (doall
       (into
        {}
        (map
         (fn [id]
           (let [s (String. ^bytes (:data (zk/data z (str "/brokers/ids/" id))))]
             {(Integer/parseInt id) (parse-string s true)}))
         broker-ids)))
      [])))

(defn starting-offset [consumer topic partition group-id task-map]
  (cond (= (:kafka/offset-reset task-map) :smallest)
        (kc/topic-offset consumer topic partition :earliest)
        (= (:kafka/offset-reset task-map) :largest)
        (kc/topic-offset consumer topic partition :latest)
        :else (kzk/committed-offset m group-id topic (str partition))))

(defn start-kafka-consumer
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [{:keys [kafka/topic kafka/partition kafka/group-id fetch-size]} task-map
        partition (str partition)
        client-id "onyx"
        m {"zookeeper.connect" (:kafka/zookeeper task-map)}
        partitions (kzk/partitions m topic)
        brokers (get partitions partition)
        broker (get (id->broker m) (first brokers))
        consumer (kc/consumer (:host broker) (:port broker) client-id)
        offset (starting-offset consumer topic partition group-id task-map)
        messages (kc/messages consumer "onyx" topic partition offset fetch-size)]
    {:kafka/consumer consumer
     :kafka/messages messages}))

(defn inject-read-messages
  [{:keys [onyx.core/task-map] :as pipeline} lifecycle]
  (let [config {"zookeeper.connect" (:kafka/zookeeper task-map)
                "group.id" (:kafka/group-id task-map)
                "auto.offset.reset" (:kafka/offset-reset task-map)
                "auto.commit.enable" "true"}
        ch (chan (:kafka/chan-capacity task-map))]
    {:kafka/future (future
                     (try
                       (log/debug "Opening Kafka resource " config)
                       (k/with-resource [c (zk/consumer config)]
                         zk/shutdown
                         (loop [ms (zk/messages c (:kafka/topic task-map))]
                           (if (= (:value (first ms)) :done)
                             (>!! ch :done)
                             (>!! ch {:content (:value (first ms))}))
                           (recur (rest ms))))
                       (catch InterruptedException e)
                       (catch Exception e
                         (fatal e)))
                     (log/debug "Stopping Kafka consumer and cleaning up"))
     :kafka/read-ch ch
     :kafka/pending-messages (atom {})}))

(defmethod p-ext/read-batch :kafka/read-messages
  [{:keys [kafka/read-ch kafka/pending-messages onyx.core/task-map] :as event}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            (if (= :done result)
                              {:id (java.util.UUID/randomUUID)
                               :input :kafka
                               :message :done}
                              {:id (java.util.UUID/randomUUID)
                               :input :kafka
                               :message (:content result)}))))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message])))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message :kafka/read-messages
  [{:keys [kafka/pending-messages onyx.core/log onyx.core/task-id]} message-id]
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/retry-message :kafka/read-messages
  [{:keys [kafka/pending-messages kafka/read-ch onyx.core/log]} message-id]
  (let [msg (get @pending-messages message-id)]
    (if (= :done (:message msg))
      (>!! read-ch :done)
      (>!! read-ch (get @pending-messages message-id))))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? :kafka/read-messages
  [{:keys [kafka/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :kafka/read-messages
  [{:keys [kafka/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (map :message (vals x))) :done))))

(defn close-read-messages
  [{:keys [kafka/read-ch] :as pipeline} lifecycle]
  (future-cancel (:kafka/future pipeline))
  (close! read-ch)
  {})

(defn inject-write-messages
  [{:keys [onyx.core/task-map] :as pipeline} lifecycle]
  (let [config {"metadata.broker.list" (:kafka/brokers task-map)
                "serializer.class" (:kafka/serializer-class task-map)
                "partitioner.class" (:kafka/partitioner-class task-map)}]
    {:kafka/config config
     :kafka/topic (:kafka/topic task-map)
     :kafka/producer (kp/producer config)}))

(defmethod p-ext/write-batch :kafka/write-messages
  [{:keys [onyx.core/results kafka/producer kafka/topic]}]
  (let [messages (mapcat :leaves results)]
    (doseq [m (map :message messages)]
      (kp/send-message producer (kp/message topic (.getBytes (pr-str m))))))
  {})

(defmethod p-ext/seal-resource :kafka/write-messages
  [{:keys [kafka/producer kafka/topic]}]
  (kp/send-message producer (kp/message topic (.getBytes (pr-str :done))))
  {})

(def read-messages-calls
  {:lifecycle/before-task inject-read-messages
   :lifecycle/after-task close-read-messages})

(def write-messages-calls
  {:lifecycle/before-task inject-write-messages})
