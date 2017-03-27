(ns onyx.plugin.partition-assignment)

;; Given a number of partitions, a number of peers, and slow for a single
;; peer, calculcates which partitions this slot should be assigned.
;; Disperses the peers over the partitions such that
;; the assignments are as evenly distributed as possible.
;;
;; First, we calculate the minimum number of partitions that
;; all peers will receive by doing division and flooring the answer.
;; Next, we take the mod of the partitions the peers, because flooring
;; the division result may have resulted in a certain number of unassigned
;; partitions. A strict subset of the peers may receive at most one more
;; partition. If a peer's slot, which is a zero-based index, is less than
;; or equal to the number of left over partitions, it can bump it's count by
;; 1 to receive one of the extras - otherwise the count remains the same.
;;
;; Now that we know how many partitions this peer's slot should receive,
;; we need to know exactly which partitions those are. Remember that partitions
;; are also zero-indexed.
;;
;; Calculating the upper-bound is easy - we take the number of partitions
;; and add it to the lower bound, decrementing by 1 to account for the zero
;; index. Thus, the only remaining variable is the lower bound.
;;
;; The lower bound is calculated by summing up all the partitions for all
;; the slots before it. We know that there are at least the base number
;; of partitions (via floored division) multiplied by this slot assigned
;; before this slot. We then need to account for the extras by adding the
;; minimum of how many partitions are left over and this slot index.
;;
;; Example: 11 partitions, 3 slots
;;
;; Slot Partitions
;;  0   0, 1, 2, 3
;;  1   4, 5, 6, 7
;;  2   8, 9, 10
;;
;; Example: 7 partitions, 3 slots
;;
;; Slot Partitions
;;  0   0, 1, 2
;;  1   3, 4
;;  2   5, 6
;;
(defn partitions-for-slot [n-partitions n-peers my-slot]
  (when (> n-peers n-partitions)
    (throw (ex-info "Number of peers assigned to this task exceeds the number of partitions in the Topic. It must be less than or equal to it."
                    {:n-partitions n-partitions
                     :n-peers n-peers})))
  (let [at-least (long (/ n-partitions n-peers))
        left-over (mod n-partitions n-peers)
        my-extra (if (<= (inc my-slot) left-over) 1 0)
        my-n-partitions (+ at-least my-extra)
        lower (+ (* at-least my-slot) (min left-over my-slot))]
    [lower (+ lower (dec my-n-partitions))]))
