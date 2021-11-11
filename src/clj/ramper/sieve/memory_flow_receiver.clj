(ns ramper.sieve.memory-flow-receiver
  (:require [ramper.sieve.flow-receiver :refer [FlowReceiver]]))

;; only using deftype as otherwise size causes problems
(deftype MemoryFlowReceiver [data-queue]
  FlowReceiver
  (prepare-to-append [_this] true)
  (append [_this hash key] (swap! data-queue conj [hash key]) true)
  (finish-appending [_this] true)
  (no-more-append [_this] true)
  (size [_this] (count @data-queue))
  (dequeue-key [_this] (-> (swap-vals! data-queue pop) first peek second))

  java.io.Closeable
  (close [_this] true))

(defn memory-flow-receiver []
  (->MemoryFlowReceiver (atom clojure.lang.PersistentQueue/EMPTY)))

(comment
  (require '[ramper.sieve.flow-receiver :refer [append dequeue-key]])

  (def mem-flow (memory-flow-receiver))
  (append mem-flow 1 "hello")
  (append mem-flow 2 "world")

  (dequeue-key mem-flow)


  )
