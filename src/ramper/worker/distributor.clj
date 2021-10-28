(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]))

(defn spawn-distributor [the-sieve sieve-receiver]
  (async/thread
    (thread-util/set-thread-name (str *ns*))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop []
      (when-let [urls (async/<!! sieve-receiver)]
        (swap! the-sieve sieve/add* urls)
        (recur)))
    (log/info :distributor :graceful-shutdown)))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])

  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))


  (def the-sieve (atom (sieve/sieve)))
  (def sieve-receiver (async/chan 100))

  (async/put! sieve-receiver (take 10 urls))

  (spawn-distributor the-sieve sieve-receiver)

  (async/close! sieve-receiver)

  (sieve/dequeue! the-sieve)

  )
