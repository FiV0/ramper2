(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]
            [ramper.workbench.simple-bench.wrapped :as bench]))

;; TODO add some limit on workbench size

(defn spawn-distributor [the-sieve the-bench sieve-receiver sieve-emitter release-chan {:keys [max-urls]}]
  (async/thread
    (thread-util/set-thread-name (str (namespace ::_)))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop [current-url nil url-count 0]
      (when-let [url (sieve/dequeue! the-sieve)]
        (bench/cons-bench the-bench url))
      (if (= url-count max-urls)
        (async/close! sieve-emitter)
        (if-let [url (or current-url (bench/dequeue! the-bench))]
          (let [[val c] (async/alts!! [[sieve-emitter url] sieve-receiver release-chan] :priority true)]
            (when val
              (cond (= c sieve-emitter)
                    (do
                      (when (= (mod url-count 100) 0) (log/info :distributor {:url-count url-count}))
                      (recur nil (inc url-count)))
                    (= c sieve-receiver)
                    (do
                      (sieve/enqueue*! the-sieve val)
                      (recur current-url url-count))
                    :else ;release-chan
                    (let [[url next-fetch] val]
                      (bench/readd the-bench url next-fetch)
                      (recur current-url url-count)))))
          ;; the timeout is to avoid deadlock in the beginning when there are
          ;; no urls in the bench yet
          (let [[val c] (async/alts!! [sieve-receiver release-chan (async/timeout 100)])]
            (cond (= c sieve-receiver)
                  (when val
                    (sieve/enqueue*! the-sieve val)
                    (recur current-url url-count))
                  (= c release-chan)
                  (let [[url next-fetch] val]
                    (bench/readd the-bench url next-fetch)
                    (recur current-url url-count))
                  :else
                  (recur current-url url-count))))))
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
    (def release-chan (async/chan 100)))

  (async/put! sieve-receiver (take 3 urls))

  (spawn-distributor the-sieve the-bench sieve-receiver sieve-emitter release-chan {:max-urls 4})

  (async/put! sieve-receiver ["foo"])

  (async/poll! sieve-emitter)

  (do
    (async/close! sieve-receiver)
    (async/close! sieve-emitter))

  )
