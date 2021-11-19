(ns ramper.sieve.mercator-sieve.wrapped
  (:require [clojure.java.io :as io]
            [ramper.sieve :refer [Sieve FlushingSieve create-sieve] :as sieve]
            [ramper.sieve.mercator-sieve :as mercator-sieve]
            [ramper.sieve.disk-flow-receiver :as receiver]
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


(defn- init-receiver []
  (receiver/disk-flow-receiver (serializer/string-byte-serializer)))

(defn hash' [x] (-> x hash long))

(defn- init-sieve [receiver store-dir]
  (mercator-sieve/mercator-seive
   true
   (let [sieve-dir (io/file store-dir "sieve")]
     (when-not (.exists sieve-dir)
       (.mkdirs sieve-dir))
     sieve-dir)
   (* 128 1024 1024) ;ca 1G
   (* 64 1024 1024)
   (* 64 1024 1024)
   receiver
   (serializer/string-byte-serializer)
   hash'))

(defn mercator-sieve []
  (let [receiver (init-receiver)]
    (->MercatorSeive (init-sieve receiver (io/file "store-dir")) receiver)))

;; TODO make store-dir configurable
(defmethod create-sieve :mercator [_ & _args]
  (mercator-sieve))

(comment
  (def mer-sieve (mercator-sieve))
  (def mer-sieve nil)

  (require '[ramper.util.url-factory :as url-factory])

  (doseq [s (url-factory/rand-str-seq 1000000)]
    (sieve/enqueue! mer-sieve s))


  (dotimes [_ 10000]
    (sieve/dequeue! mer-sieve))

  (doseq [s (url-factory/rand-str-seq 1000000)]
    (sieve/enqueue! mer-sieve s))

  (sieve/enqueue! mer-sieve "abc")
  (sieve/enqueue! mer-sieve "bcd")
  (sieve/enqueue*! mer-sieve ["bcd" "ddddd"])
  (sieve/flush! mer-sieve)

  (sieve/dequeue! mer-sieve)

  (.close mer-sieve)

  )
