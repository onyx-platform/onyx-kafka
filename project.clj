(defproject org.onyxplatform/onyx-kafka "0.9.10.0-beta5"
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
                 [org.onyxplatform/onyx "0.9.10-beta5"]
                 [ymilky/franzy "0.0.1"]
                 [com.stuartsierra/component "0.2.3"]]
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark
                   :all (constantly true)}
  :profiles {:dev {:dependencies [[cheshire "5.5.0"]
                                  [zookeeper-clj "0.9.3" :exclusions [io.netty/netty org.apache.zookeeper/zookeeper]]
                                  [aero "0.2.0"]
                                  [prismatic/schema "1.0.5"]
                                  [ymilky/franzy-embedded "0.0.1" :exclusions [org.slf4j/slf4j-log4j12]]
                                  [ymilky/franzy-admin "0.0.1" :exclusions [org.slf4j/slf4j-log4j12]]] 
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :global-vars  {*warn-on-reflection* true
                                  *assert* false
                                  *unchecked-math* :warn-on-boxed}
                   :java-opts ^:replace ["-server"
                                         "-Xmx4g"
                                         ;"-XX:+UnlockCommercialFeatures" 
                                         ;"-XX:+FlightRecorder"
                                         ;"-XX:+UnlockDiagnosticVMOptions"
                                         ;"-XX:StartFlightRecording=duration=1080s,filename=localrecording.jfr"
                                         ]}})
