(defproject org.onyxplatform/onyx-kafka "0.7.3-SNAPSHOT"
  :description "Onyx plugin for Kafka"
  :url "https://github.com/MichaelDrogalis/onyx-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.7.3-20150828_160954-g51b5cf8"]
                 [clj-kafka "0.3.2" :exclusions [org.apache.zookeeper/zookeeper zookeeper-clj]]
                 [com.stuartsierra/component "0.2.3"]
                 [cheshire "5.5.0"]
                 [zookeeper-clj "0.9.3" :exclusions [io.netty/netty org.apache.zookeeper/zookeeper]]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
