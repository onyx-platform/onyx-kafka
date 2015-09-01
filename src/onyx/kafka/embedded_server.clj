(ns onyx.kafka.embedded-server
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error] :as timbre])
  (:import [kafka.utils SystemTime$]
           [kafka.server KafkaConfig KafkaServer]
           [java.util Properties]))

(defrecord EmbeddedKafka [hostname port broker-id num-partitions
                          zookeeper-addr log-dir]
  component/Lifecycle
  (component/start [component]
    (let [properties (doto (Properties.)
                       (.setProperty "broker.id" (str broker-id))
                       (.setProperty "hostname" hostname)
                       (.setProperty "num.partitions" (str (or num-partitions 1)))
                       (.setProperty "port" (str port))
                       (.setProperty "log.dir" (or log-dir
                                                   (str "/tmp/kafka-log-" (java.util.UUID/randomUUID))))
                       (.setProperty "controlled.shutdown.enable" "false")
                       (.setProperty "zookeeper.connect" zookeeper-addr))
          kafka-config (KafkaConfig. properties)
          server (KafkaServer. kafka-config SystemTime$/MODULE$)]
      (.startup server)
      (assoc component :server server)))
  (component/stop [{:keys [server] :as component}]
    (.shutdown ^KafkaServer server)
    (.awaitShutdown ^KafkaServer server)
    (assoc component :server nil)))


