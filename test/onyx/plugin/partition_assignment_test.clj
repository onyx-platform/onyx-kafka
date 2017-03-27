(ns onyx.plugin.partition-assignment-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
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

(deftest full-partition-coverage
  (checking
   "All partitions are assigned"
   (times 50000)
   [[parts slots]
    (gen/bind gen/s-pos-int
              (fn [parts]
                (gen/bind (gen/resize (dec parts) gen/s-pos-int)
                          (fn [slots] (gen/return [parts slots])))))]
   (is
    (= parts
       (reduce
        (fn [sum slot]
          (let [[lower upper] (partitions-for-slot parts slots slot)]
            (+ sum (inc (- upper lower)))))
        0
        (range slots))))))

(deftest contiguous-assignments
  (checking
   "Partitions are contiguously assigned"
   (times 50000)
   [[parts slots]
    (gen/bind gen/s-pos-int
              (fn [parts]
                (gen/bind (gen/resize (dec parts) gen/s-pos-int)
                          (fn [slots] (gen/return [parts slots])))))]
   (let [partitions (map (partial partitions-for-slot parts slots) (range slots))]
     (doseq [[a b] (partition 2 1 partitions)]
       (is (= (second a) (dec (first b))))))))
