(ns onyx.kafka.information-model)

(def model
  {:catalog-entry
   {:kafka/read-messages
    {:summary "An input task to read messages from a Kafka topic."
     :model {:kafka/topic
             {:doc "The topic name to read from."
              :type :string}

             :kafka/group-id
             {:doc "The consumer identity to store in ZooKeeper."
              :type :string}

             :kafka/partition
             {:doc "Partition to read from if auto-assignment is not used."
              :type :string
              :optional? true}

             :kafka/zookeeper
             {:doc "The ZooKeeper connection string."
              :type :string}

             :kafka/offset-reset
             {:doc "Offset bound to seek to when not found - `:smallest` or `:largest`."
              :type :keyword}

             :kafka/force-reset?
             {:doc "Force to read from the beginning or end of the log, as specified by `:kafka/offset-reset`. If false, reads from the last acknowledged messsage if it exists."
              :type :boolean}

             :kafka/chan-capacity
             {:doc "The buffer size of the Kafka reading channel."
              :type :long
              :default 1000
              :optional? true}

             :kafka/fetch-size
             {:doc "The size in bytes to request from ZooKeeper per fetch request."
              :type :long
              :default 307200
              :optional? true}

             :kafka/empty-read-back-off
             {:doc "The amount of time to back off between reads when nothing was fetched from a consumer."
              :type :long
              :default 500
              :optional? true}

             :kafka/commit-interval
             {:doc "The interval in milliseconds to commit the latest acknowledged offset to ZooKeeper."
              :type :long
              :default 2000
              :optional? true}

             :kafka/deserializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to deserialize a message. Takes one argument, which must be a byte array."
              :type :keyword}

             :kafka/wrap-with-metadata?
             {:doc "Wraps message into map with keys `:offset`, `:partitions`, `:topic` and `:message` itself."
              :type :boolean
              :default false
              :optional? true}

             :kafka/request-size
             {:doc "The maximum size of request messages.  Maps to the `max.request.size` value of the internal kafka producer."
              :type :long
              :default 307200
              :optional? true}}}

    :kafka/write-messages
    {:summary "Write messages to kafka."
     :model {:kafka/topic
             {:doc "The topic name to write to. Must either be supplied or otherwise all messages must contain a `:topic` key"
              :optional? true
              :type :string}

             :kafka/partition
             {:doc "Partition to write to, if you do not wish messages to be auto allocated to partitions. Must either be supplied in the task map, or all messages should contain a `:partition` key."
              :type :string
              :optional? true}

             :kafka/zookeeper
             {:doc "The ZooKeeper connection string."
              :type :string}

             :kafka/serializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to serialize a message. Takes one argument - the segment."
              :type :keyword}

             :kafka/no-seal?
             {:doc "Do not write :done to the topic when task receives the sentinel signal (end of batch job)."
              :type :boolean
              :default false
              :optional? true}}}}

   :lifecycle-entry
   {:kafka/read-messages
    {:model
     [{:lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]}

    :kafka/write-messages
    {:model
     [{:lifecycle/calls :onyx.plugin.kafka/write-messages-calls}]}}

   :display-order
   {:kafka/read-messages
    [:kafka/topic
     :kafka/partition
     :kafka/group-id
     :kafka/zookeeper
     :kafka/offset-reset
     :kafka/force-reset?
     :kafka/deserializer-fn
     :kafka/chan-capacity
     :kafka/request-size
     :kafka/fetch-size
     :kafka/empty-read-back-off
     :kafka/commit-interval
     :kafka/wrap-with-metadata?]

    :kafka/write-messages
    [:kafka/topic
     :kafka/zookeeper
     :kafka/partition
     :kafka/serializer-fn
     :kafka/no-seal?]}})
