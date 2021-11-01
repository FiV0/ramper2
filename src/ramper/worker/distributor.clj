(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]))

(defn spawn-distributor [the-sieve sieve-receiver sieve-emitter max-urls]
  (async/thread
    (thread-util/set-thread-name (str (namespace ::_)))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop [current-url nil url-count 0]
      (if (= url-count max-urls)
        (async/close! sieve-emitter)
        (if-let [url (or current-url (sieve/dequeue! the-sieve))]
          (let [[val c] (async/alts!! [sieve-receiver [sieve-emitter url]])]
            (when val
              (if (= c sieve-emitter)
                (recur nil (inc url-count))
                (do
                  (sieve/enqueue*! the-sieve val)
                  (recur current-url url-count)
                  ))))
          (when-let [urls (async/<!! sieve-receiver)]
            (sieve/enqueue*! the-sieve urls)
            (recur current-url url-count)))))
    (log/info :distributor :graceful-shutdown)))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])
  (require '[ramper.sieve.memory-sieve :as mem-sieve])

  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def the-sieve (mem-sieve/memory-sieve))
  (def sieve-receiver (async/chan 100))
  (def sieve-emitter (async/chan 100))

  (async/put! sieve-receiver (take 10 urls))

  (spawn-distributor the-sieve sieve-receiver sieve-emitter 11)

  (async/put! sieve-receiver ["foo"])

  (async/poll! sieve-emitter)

  (do
    (async/close! sieve-receiver)
    (async/close! sieve-emitter))

  )
