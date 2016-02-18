(defproject org.onyxplatform/onyx-kafka "0.8.10.2-SNAPSHOT"
  :description "Onyx plugin for Kafka"
  :url "https://github.com/onyx-platform/onyx-kafka"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.8.12-20160218_022348-g82e00f3"]
                 [clj-kafka "0.3.4" :exclusions [org.apache.zookeeper/zookeeper zookeeper-clj]]
                 [com.stuartsierra/component "0.2.3"]
                 [cheshire "5.5.0"]
                 [zookeeper-clj "0.9.3" :exclusions [io.netty/netty org.apache.zookeeper/zookeeper]]
                 [prismatic/schema "1.0.5"]]
  :profiles {:dev {:dependencies [[midje "1.7.0"]]
                   :plugins [[lein-midje "3.1.3"]
                             [lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
