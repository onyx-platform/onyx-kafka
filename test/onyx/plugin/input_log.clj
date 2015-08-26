(ns onyx.plugin.input-log
  (:require [onyx.plugin.kafka-log :as kl]
            [midje.sweet :refer :all]))

(fact "Allocate from scratch"
      (kl/allocate-partition {:allocations {:task-a [:peer-3]}}
                             {:n-partitions 5
                              :task-id :task-a 
                              :peer-id :peer-3})
      => 
      {:task-metadata {:task-a {:peer-3 0}}
       :allocations {:task-a [:peer-3]}})

(fact "Allocate, however peer is no longer allocated to task"
      (kl/allocate-partition {:allocations {:task-a []}}
                             {:n-partitions 5
                              :task-id :task-a 
                              :peer-id :peer-3})
      => 
      {:allocations {:task-a []}})

(fact "Allocate a new peer, deallocate dropped peer"
      (kl/allocate-partition {:task-metadata {:task-a {:peer-0 1
                                                       :peer-1 0 
                                                       :peer-2 3}}
                              :allocations {:task-a [:peer-1 
                                                     :peer-2
                                                     :peer-3]}}
                             {:n-partitions 5
                              :task-id :task-a 
                              :peer-id :peer-3})
      => 
      {:task-metadata {:task-a {:peer-1 0 
                                :peer-2 3 
                                :peer-3 1}}
       :allocations {:task-a [:peer-1 :peer-2 :peer-3]}})
