(ns onyx.kafka.embedded-server
  (:require [com.stuartsierra.component :as component]
            [franzy.embedded.component :refer [make-embedded-startable-broker]]
            [franzy.embedded.server :as server]
            [taoensso.timbre :refer [info error] :as timbre]))

(defrecord EmbeddedStartableBroker [broker-config]
  component/Lifecycle
  (start [component]
    (timbre/info "Starting embedded startable Kafka component..." broker-config)
    (let [server (server/make-startable-server broker-config)]
      (.startup server)
      (assoc component
             :server server)))
  (stop [{:keys [server] :as component}]
    (timbre/info "Stopping embedded startable Kafka component..." broker-config)
    (doto server
      (.shutdown)
      (.awaitShutdown))
    (timbre/info "Stopped embedded startable Kafka component.")
    (assoc component
           :server nil)))

(defn embedded-kafka [opts]
  (->EmbeddedStartableBroker opts))
