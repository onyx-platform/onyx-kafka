(defproject com.mdrogalis/onyx-kafka "0.3.1"
  :description "Onyx plugin for Kafka"
  :url "https://github.com/MichaelDrogalis/onyx-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/data.fressian "0.2.0"]
                 [com.mdrogalis/onyx "0.3.1"]
                 [com.taoensso/timbre "3.0.1"]
                 [clj-kafka "0.2.6-0.8" :exclusions [org.apache.zookeeper/zookeeper]]]
  :profiles {:dev {:dependencies [[com.mdrogalis/onyx-core-async "0.3.1"]
                                  [midje "1.6.2"]]
                   :plugins [[lein-midje "3.1.3"]]}})
