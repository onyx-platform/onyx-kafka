(ns onyx.plugin.kafka-log
  (:require [onyx.extensions :as extensions]))

(defn allocate-partition [replica {:keys [job-id task-id peer-id n-partitions] :as args}]
  (let [task-allocations (get-in (:allocations replica) [job-id task-id])] 
    (if (some #{peer-id} task-allocations)
      (let [kafka-allocations (-> replica 
                                  (get-in [:task-metadata job-id task-id])
                                  (select-keys task-allocations))
            remaining-partitions (remove (set (vals kafka-allocations)) (range n-partitions))
            new-kafka-allocations (assoc kafka-allocations peer-id (first remaining-partitions))] 
        (assoc-in replica [:task-metadata job-id task-id] new-kafka-allocations))
      replica)))

(defmethod extensions/apply-log-entry :allocate-kafka-partition
  [{:keys [args]} replica]
  (allocate-partition replica args))

(defmethod extensions/replica-diff :allocate-kafka-partition
  [entry old new]
  {})

(defmethod extensions/reactions :allocate-kafka-partition
  [{:keys [args]} old new diff state]
  [])

(defmethod extensions/fire-side-effects! :allocate-kafka-partition
  [{:keys [args]} old new diff state]
  ;; it may be possible to place the reader-loop future in here
  ;; currently there are too many parameters missing, and it will be awkward
  ;; to stop the future on shutdown
  state)
