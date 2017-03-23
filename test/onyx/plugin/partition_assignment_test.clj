(ns onyx.plugin.partition-assignment-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.plugin.partition-assignment :refer [partitions-for-slot]]))

(deftest test-slot-assigment
  (let [parts 1
        slots 1]
    (is (= [0 0] (partitions-for-slot parts slots 0))))

  (let [parts 3
        slots 1]
    (is (= [0 2] (partitions-for-slot parts slots 0))))

  (let [parts 4
        slots 2]
    (is (= [0 1] (partitions-for-slot parts slots 0)))
    (is (= [2 3] (partitions-for-slot parts slots 1))))

  (let [parts 4
        slots 3]
    (is (= [0 1] (partitions-for-slot parts slots 0)))
    (is (= [2 2] (partitions-for-slot parts slots 1)))
    (is (= [3 3] (partitions-for-slot parts slots 2))))
  
  (let [parts 7
        slots 3]
    (is (= [0 2] (partitions-for-slot parts slots 0)))
    (is (= [3 4] (partitions-for-slot parts slots 1)))
    (is (= [5 6] (partitions-for-slot parts slots 2))))
  
  (let [parts 11
        slots 3]
    (is (= [0 3] (partitions-for-slot parts slots 0)))
    (is (= [4 7] (partitions-for-slot parts slots 1)))
    (is (= [8 10] (partitions-for-slot parts slots 2)))))
