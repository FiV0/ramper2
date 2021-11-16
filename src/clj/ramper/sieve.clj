(ns ramper.sieve)

(defprotocol Sieve
  "A Sieve guarantees the following property: every key that is enqueued gets dequeued once,
  and once only. It sort of works like a unique filter."
  (enqueue! [this key] "Add the given key to the sieve.")
  (enqueue*! [this keys] "Add the given keys to the sieve.")
  (dequeue! [this] "Dequeues a key from the sieve."))

(defprotocol FlushingSieve
  "A flushing sieve is a sieve that does some heavier computations from time to time.
  Most likely IO to disk."
  (flush! [this] "Flushes all pending enqueued keys to the underlying medium.")
  (last-flush [this] "Returns a timestamp of the last sieve flush."))

(defprotocol Size
  "Generic protocol to get the size (number of items) in the data structure."
  (number-of-items [this]))

(defmulti create-sieve (fn [type & _args] type))

(defmethod create-sieve :default [type & _args]
  (throw (IllegalArgumentException. (str "No such sieve: " type))))

(comment
  (require '[ramper.sieve.memory-sieve :as sieve])

  (def mem-sieve (sieve/memory-sieve))

  (enqueue! mem-sieve "hello.world")
  (enqueue*! mem-sieve '("this" "is" "good"))
  (dequeue! mem-sieve)

  )
