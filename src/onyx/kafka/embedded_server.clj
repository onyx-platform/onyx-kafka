(ns onyx.kafka.embedded-server
  (:require [com.stuartsierra.component :as component])
  (:import [kafka.utils SystemTime$]
           [kafka.server KafkaConfig KafkaServer]
           [java.util Properties]))

(defrecord EmbeddedKafka [hostname port broker-id num-partitions zookeeper-addr log-dir]
  component/Lifecycle
  (component/start [component]
    (let [properties (doto (Properties.)
                       (.setProperty "broker.id" (str broker-id))
                       (.setProperty "hostname" hostname)
                       (.setProperty "num.partitions" (str (or num-partitions 1)))
                       (.setProperty "port" (str port))
                       (.setProperty "log.dir" (or log-dir "/tmp/kafka-logs"))
                       (.setProperty "zookeeper.connect" zookeeper-addr))
          kafka-config (KafkaConfig. properties)
          server (KafkaServer. kafka-config SystemTime$/MODULE$)]
      (.startup server)
      (assoc component :server server)))
  (component/stop [{:keys [server] :as component}]
    (.shutdown server)
    (.awaitShutdown server)
    (assoc component :server nil)))


