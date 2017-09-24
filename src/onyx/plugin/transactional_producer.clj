(ns onyx.plugin.transactional-producer)

(defn invoke [object method-name args]
  (let [arg-types (into-array Class (map (fn [v] (.getClass v)) args))
        method (.getDeclaredMethod (.getClass object) method-name arg-types)]
    (.setAccessible method true)
    (.invoke method object (into-array Object args))))

(defn set-value [object field-name value]
  (let [field (.getDeclaredField (.getClass object) field-name)]
    (.setAccessible field true)
    (.set field object value)))

(defn state-enum [s]
  (Enum/valueOf (Class/forName "org.apache.kafka.clients.producer.internals.TransactionManager$State") s))

(defn get-value 
  ([object field-name]
   (get-value object (.getClass object) field-name))
  ([object cla field-name]
   (try 
    (let [field (.getDeclaredField cla field-name)]
      (.setAccessible field true)
      (.get field object))
    (catch NoSuchFieldException nsfe
      (throw (ex-info "Incompatible producer version." {:e nsfe})))
    (catch IllegalAccessException iae
      (throw (ex-info "Incompatible producer version." {:e iae}))))))

(defn transition-producer [tm state]
  (invoke tm "transitionTo" [state]))

(defprotocol KafkaProducerFinalCommitter 
  (epoch [producer])
  (producer-id [producer])
  (resume-transaction [producer producer-id epoch]))

(defn producer-epoch [producer]
  (let [tm (get-value producer "transactionManager")]
    (get-value tm "producerIdAndEpoch")))

(extend-type org.apache.kafka.clients.producer.KafkaProducer 
  KafkaProducerFinalCommitter
  (producer-id [producer]
    (short (get-value (producer-epoch producer) "producerId")))
  (epoch [producer]
    (short (get-value (producer-epoch producer) "epoch")))
  (resume-transaction [producer producer-id epoch]
    (let [tm (get-value producer "transactionManager")
          pe (get-value tm "producerIdAndEpoch")
          sequence-numbers (get-value tm "sequenceNumbers")]
      (transition-producer (get-value producer "transactionManager") (state-enum "INITIALIZING"))
      (invoke sequence-numbers "clear" [])
      (set-value pe "producerId" producer-id)
      (set-value pe "epoch" epoch)
      (transition-producer (get-value producer "transactionManager") (state-enum "READY"))
      (transition-producer (get-value producer "transactionManager") (state-enum "IN_TRANSACTION"))
      (set-value tm "transactionStarted" true))))
