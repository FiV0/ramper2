(ns ramper.sieve.mercator-sieve.wrapped
  (:require [clojure.java.io :as io]
            [ramper.sieve :refer [Sieve FlushingSieve] :as sieve]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.sieve.flow-receiver :as receiver]
            [ramper.sieve.disk-flow-receiver :as disk-receiver]
            [ramper.sieve.memory-flow-receiver :as memory-receiver]
            [ramper.util.byte-serializer :as serializer]))

(defrecord MercatorSeive [sieve receiver]
  Sieve
  (enqueue! [_this key] (sieve/enqueue! sieve key))
  (enqueue*! [_this keys] (sieve/enqueue*! sieve keys))
  (dequeue! [_this] (receiver/dequeue-key receiver))

  FlushingSieve
  (flush! [_this] (sieve/flush! sieve))
  (last-flush [_this] (sieve/last-flush sieve))

  java.io.Closeable
  (close [_this]
    (.close sieve)
    (.close receiver)))


(defn- init-receiver [type]
  (case type
    :memory (memory-receiver/memory-flow-receiver)
    (disk-receiver/disk-flow-receiver (serializer/string-byte-serializer))))

(defn hash' [x] (-> x hash long))

(defn- init-sieve [receiver]
  (mercator-sieve/mercator-seive
   true
   (io/file "store-dir/sieve")
   (* 64 1024)
   (* 64 1024)
   (* 64 1024)
   receiver
   (serializer/string-byte-serializer)
   hash'))

(defn mercator-sieve []
  (let [receiver (init-receiver :memory)]
    (->MercatorSeive (init-sieve receiver) receiver)))

(comment
  (def mer-sieve (mercator-sieve))

  (sieve/enqueue! mer-sieve "abc")
  (sieve/enqueue! mer-sieve "bcd")
  (sieve/enqueue*! mer-sieve ["bcd" "ddddd"])
  (sieve/flush! mer-sieve)

  (sieve/dequeue! mer-sieve)

  )
