(ns ramper.sieve.db-sieve.wrapped
  (:require [ramper.sieve :refer [Sieve]]
            [ramper.sieve.db-sieve :as db-sieve]))

(deftype DbSieveWrapped [^:volatile-mutable sieve]
  Sieve
  (enqueue! [this key]
    (locking this
      (set! sieve (db-sieve/enqueue! sieve key))))

  (enqueue*! [this keys]
    (locking this
      (set! sieve (db-sieve/enqueue*! sieve keys))))

  (dequeue! [this]
    (locking this
      (when-let [res (db-sieve/peek-sieve sieve)]
        (set! sieve (db-sieve/pop-sieve sieve))
        res))))

(defn db-sieve-factory []
  (->DbSieveWrapped (db-sieve/db-sieve)))

(comment
  (require '[ramper.sieve :refer [enqueue! enqueue*! dequeue!]])
  (def sieve (db-sieve-factory))

  (enqueue! sieve "abc")
  (enqueue! sieve "bcd")
  (enqueue*! sieve ["abd" "bcd" "abc"])

  (dequeue! sieve)
  )
