(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]))

(defn spawn-distributor [the-sieve sieve-receiver sieve-emitter]
  (async/thread
    (thread-util/set-thread-name (str *ns*))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop []
      (if-let [url (sieve/peek-sieve @the-sieve)]
        (when-let [[val c] (async/alts!! [sieve-receiver [sieve-emitter url]])]
          (if (= c sieve-emitter)
            (swap! the-sieve sieve/pop-sieve)
            (do
              (swap! the-sieve sieve/add* val)
              #_(log/info :distributor1 {:urls-size (count val)})))
          (recur))
        (when-let [urls (async/<!! sieve-receiver)]
          (swap! the-sieve sieve/add* urls)
          #_(log/info :distributor2 {:urls-size (count urls)})
          (recur))))
    (log/info :distributor :graceful-shutdown)))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])

  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def the-sieve (atom (sieve/sieve)))
  (def sieve-receiver (async/chan 100))
  (def sieve-emitter (async/chan 100))

  (async/put! sieve-receiver (take 10 urls))

  (spawn-distributor the-sieve sieve-receiver sieve-emitter)

  (async/poll! sieve-emitter)

  (do
    (async/close! sieve-receiver)
    (async/close! sieve-emitter))

  )
