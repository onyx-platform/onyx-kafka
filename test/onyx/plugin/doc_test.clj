(ns onyx.plugin.doc-test
  (:require [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.kafka.information-model :refer [model]]))

(deftest check-model-display-order
  (testing "Checks whether all keys in information model are accounted for in ordering used in cheat sheet"
    (let [{:keys [catalog-entry display-order]} model]
      (doall 
        (for [[task-type task-info] catalog-entry]
          (let [display-order (get display-order task-type)]
            (is (= (set display-order)
                   (set (keys (:model task-info)))))))))))
