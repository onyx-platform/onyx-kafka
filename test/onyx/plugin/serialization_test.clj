(ns onyx.plugin.serialization-test
  (:require [clojure.core.async :refer [<!! go pipe close! >!!]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.kafka.helpers :as h]
            [onyx.tasks.kafka :refer [producer consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kafka]
            [onyx.api]
            [taoensso.timbre :as log])
  (:import [org.apache.kafka.common.serialization LongSerializer LongDeserializer StringSerializer StringDeserializer]
           [org.apache.kafka.clients.consumer ConsumerRecords]))

(def key-serializer-name (.getName ^Class LongSerializer))
(def key-deserializer-name (.getName ^Class LongDeserializer))

(def value-serializer-name (.getName ^Class StringSerializer))
(def value-deserializer-name (.getName ^Class StringDeserializer))

(def batch-size 10)
(def batch-timeout 1000)

(def batch-settings {:onyx/batch-size    batch-size
                     :onyx/batch-timeout batch-timeout})

(defn key-serializer [key]
  (is (instance? Long key))
  key)

(defn value-serializer [value]
  (is (instance? String value))
  value)

(defn custom-serializers-producer-task [zk-address topic]
  (producer :write-messages
            (merge {:kafka/topic topic
                    :kafka/zookeeper zk-address

                    :kafka/serializer value-serializer-name
                    :kafka/serializer-fn ::value-serializer
                    :kafka/key-serializer key-serializer-name
                    :kafka/key-serializer-fn ::key-serializer

                    :kafka/request-size 307200}
                   batch-settings)))

(defn build-write-messages-job [zk-address topic]
  (let [base-job (merge {:workflow   [[:in :identity]
                                      [:identity :write-messages]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn :clojure.core/identity
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (core-async/input :in batch-settings))
        (add-task (custom-serializers-producer-task zk-address topic)))))

(defn custom-deserializers-consumer-task [zk-address topic wrap-with-metadata]
  (consumer :read-messages (merge {:kafka/topic topic
                                   :kafka/group-id "onyx-consumer"
                                   :kafka/zookeeper zk-address
                                   :kafka/offset-reset :earliest

                                   :kafka/wrap-with-metadata? wrap-with-metadata

                                   :kafka/deserializer value-deserializer-name
                                   :kafka/deserializer-fn ::value-serializer
                                   :kafka/key-deserializer key-deserializer-name
                                   :kafka/key-deserializer-fn ::key-serializer

                                   :onyx/min-peers 1
                                   :onyx/max-peers 1}
                                  batch-settings)))

(defn assert-read-messages [message]
  message)

(defn build-read-messages-job [zk-address topic wrap-with-metadata]
  (let [base-job (merge {:workflow [[:read-messages :assert-read-messages]
                                    [:assert-read-messages :out]]
                         :catalog [(merge {:onyx/name :assert-read-messages
                                           :onyx/fn ::assert-read-messages
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (custom-deserializers-consumer-task zk-address topic wrap-with-metadata))
        (add-task (core-async/output :out batch-settings)))))

(defn check-vanilla-kafka-consumer-is-happy-with [topic bootstrap-servers]
  (let [my-consumer 
        (h/build-consumer {"bootstrap.servers" bootstrap-servers
                           "group.id" (str "vanilla-java" (java.util.UUID/randomUUID))
                           "key.deserializer" key-deserializer-name
                           "value.deserializer" value-deserializer-name})]

    (.subscribe my-consumer [topic])
    (.poll my-consumer 100)

    (h/seek-to-beginning! my-consumer [{:topic topic :partition 0}])
  
    (let [records (.poll my-consumer 2000)]
      (is (= 3 (.count ^ConsumerRecords records)))

      (let [key (.key (first records))
            value (.value (first records))] 
        (is (= java.lang.Long (type key)))
        (is (= java.lang.String (type value)))
        (is (= 1 key))
        (is (= "Message 1" value))))))

(deftest kafka-serialization-test
  (let [test-topic (str "onyx-test-" (java.util.UUID/randomUUID))
        test-timestamp (System/currentTimeMillis)
        {:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID))
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        write-job (build-write-messages-job zk-address test-topic)
        bootstrap-servers (:kafka-bootstrap test-config)
        {:keys [in]} (get-core-async-channels write-job)
        test-data [{:key 1 :message "Message 1"}
                   {:key 2 :message "Message 2"}
                   {:key 3 :message "Message 3"}]]
    (with-test-env [test-env [4 env-config peer-config]]
      (onyx.test-helper/validate-enough-peers! test-env write-job)
      (h/create-topic! zk-address test-topic 1 1)
      (run! #(>!! in %) test-data)
      (close! in)
      (println "Seeding topic" test-topic)

      (let [job-id (:job-id (onyx.api/submit-job peer-config write-job))]
        (onyx.test-helper/feedback-exception! peer-config job-id)
        (println "letting write job complete")
        (onyx.api/await-job-completion peer-config job-id)
        (println "... done"))

      (testing "vanilla KafkaConsumer is happy"
        (check-vanilla-kafka-consumer-is-happy-with test-topic bootstrap-servers))

      (testing "reading with custom serializers"
        (let [read-job      (build-read-messages-job zk-address test-topic false)
              _             (onyx.test-helper/validate-enough-peers! test-env read-job)
              job-id        (:job-id (onyx.api/submit-job peer-config read-job))
              {:keys [out]} (get-core-async-channels read-job)
              results       (onyx.plugin.core-async/take-segments! out 10000)]
          (println "Result" results)
          (is (= ["Message 1" "Message 2" "Message 3"] results))
          (println "Done taking segments")
          (onyx.api/kill-job peer-config job-id)))

      (testing "reading with custom serializers and {:wrap-with-metadata? true}"
        (let [read-job      (build-read-messages-job zk-address test-topic true)
              _             (onyx.test-helper/validate-enough-peers! test-env read-job)
              job-id        (:job-id (onyx.api/submit-job peer-config read-job))
              {:keys [out]} (get-core-async-channels read-job)
              results       (onyx.plugin.core-async/take-segments! out 10000)]
          (println "Result" results)
          (let [first-message (first results)
                {:keys [:key :message :topic]} first-message]
            (is (= 1 key))
            (is (= "Message 1" message))
            (is (= test-topic topic)))
          (println "Done taking segments")
          (onyx.api/kill-job peer-config job-id))))))
