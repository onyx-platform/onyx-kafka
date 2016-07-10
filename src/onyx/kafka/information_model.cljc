(ns onyx.kafka.information-model)

(def model
  {:catalog-entry
   {:model {:kafka/topic
            {:doc "The topic name to read from or write to."
             :type :string}

            :kafka/partition
            {:doc "Partition to read from if auto-assignment is not used."
             :type :string
             :optional? true}

            :kafka/group-id
            {:doc "The consumer identity to store in ZooKeeper."
             :type :string}

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

            :kafka/serializer-fn
            {:doc "A keyword that represents a fully qualified namespaced function to serialize a message. Takes one argument - the segment."
             :type :keyword}


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
             :optional? true}

            :kafka/no-seal?
            {:doc "Do not write :done to the topic when task receives the sentinel signal (end of batch job)."
             :type :boolean
             :default false
             :optional? true}}}

   :task-bundles
   {:kafka/read-messages
    [:kafka/topic
     :kafka/group-id
     :kafka/zookeeper
     :kafka/offset-reset
     :kafka/force-reset?
     :kafka/deserializer-fn
     :kafka/partition
     :kafka/chan-capacity
     :kafka/fetch-size
     :kafka/empty-read-back-off
     :kafka/commit-interval
     :kafka/wrap-with-metadata?]

    :kafka/write-messages
    [:kafka/topic
     :kafka/zookeeper
     :kafka/serializer-fn
     :kafka/request-size
     :kafka/no-seal?]}})
