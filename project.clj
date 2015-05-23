(defproject com.mdrogalis/onyx-kafka "0.6.0.1-beta1"
  :description "Onyx plugin for Kafka"
  :url "https://github.com/MichaelDrogalis/onyx-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [com.mdrogalis/onyx "0.6.0-beta1" :exclusions [org.clojure/core.async]]
                 [com.taoensso/timbre "3.0.1"]
                 [clj-kafka "0.2.8-0.8.1.1" :exclusions [org.apache.zookeeper/zookeeper zookeeper-clj]]
                 [cheshire "5.4.0"]
                 [zookeeper-clj "0.9.1" :exclusions [io.netty/netty org.apache.zookeeper/zookeeper]]]
  :profiles {:dev {:dependencies [[midje "1.6.2" :exclusions [commons-codec]]]
                   :plugins [[lein-midje "3.1.3"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
