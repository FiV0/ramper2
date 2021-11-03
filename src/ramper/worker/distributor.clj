(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]
            [ramper.workbench.simple-bench.wrapped :as bench]))

;; TODO add some limit on workbench size

(defn spawn-distributor [the-sieve the-bench sieve-receiver sieve-emitter release-chan {:keys [max-urls counter]}]
  (async/go
    (loop [current-url nil]
      (when-let [url (sieve/dequeue! the-sieve)]
        (bench/cons-bench the-bench url))
      (if (= @counter max-urls)
        (async/close! sieve-emitter)
        (if-let [url (or current-url (bench/dequeue! the-bench))]
          (let [[val c] (async/alts!! [[sieve-emitter url] sieve-receiver release-chan] :priority true)]
            (when val
              (cond (= c sieve-emitter)
                    (do
                      (swap! counter inc)
                      (when (= (mod @counter 100) 0) (log/info :distributor {:url-count @counter}))
                      (recur nil))
                    (= c sieve-receiver)
                    (do
                      (sieve/enqueue*! the-sieve val)
                      (recur current-url))
                    :else ;release-chan
                    (let [[url next-fetch] val]
                      (bench/readd the-bench url next-fetch)
                      (recur current-url)))))
          ;; the timeout is to avoid deadlock in the beginning when there are
          ;; no urls in the bench yet
          (let [[val c] (async/alts!! [sieve-receiver release-chan (async/timeout 100)])]
            (cond (= c sieve-receiver)
                  (when val
                    (sieve/enqueue*! the-sieve val)
                    (recur current-url))
                  (= c release-chan)
                  (let [[url next-fetch] val]
                    (bench/readd the-bench url next-fetch)
                    (recur current-url))
                  :else
                  (recur current-url))))))
    (log/info :distributor :graceful-shutdown)))

(comment
  (do
    (require '[clojure.java.io :as io])
    (require '[ramper.util :as util])
    (require '[ramper.sieve.memory-sieve :as mem-sieve])

    (def urls (util/read-urls (io/file (io/resource "seed.txt")))))

  (do
    (def the-sieve (mem-sieve/memory-sieve))
    (def the-bench (bench/simple-bench-factory))
    (def sieve-receiver (async/chan 100))
    (def sieve-emitter (async/chan 100))
    (def release-chan (async/chan 100))
    (def counter (atom 0)))

  (async/put! sieve-receiver (take 3 urls))

  (spawn-distributor the-sieve the-bench sieve-receiver sieve-emitter release-chan {:max-urls 4 :counter counter})

  (async/put! sieve-receiver ["foo"])

  (async/poll! sieve-emitter)

  (do
    (async/close! sieve-receiver)
    (async/close! sieve-emitter))

  )
