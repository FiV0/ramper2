(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve]
            [ramper.workbench.simple-bench.wrapped :as workbench]))

(def ^:private the-ns-name (str *ns*))

(defn spawn-distributor [the-sieve the-bench sieve-receiver sieve-emitter max-urls]
  (async/thread
    (thread-util/set-thread-name (str the-ns-name))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop [current-url nil url-count 0]
      (if (= url-count max-urls)
        (async/close! sieve-emitter)
        (if-let [url (or current-url (workbench/dequeue! the-bench))]
          (let [[val c] (async/alts!! [sieve-receiver [sieve-emitter url]])]
            (when val
              (if (= c sieve-emitter)
                (do
                  (log/debug :distributor {:emitted url})
                  (recur nil (inc url-count)))
                (do
                  (log/debug :distributor1 {:enqueued (count val)})
                  (sieve/enqueue*! the-sieve val)
                  (recur current-url url-count)))))
          ;; the timeout is to avoid deadlock in the beginning when there
          ;; are no urls in the bench yet
          (let [timeout-chan (async/timeout 100)
                [urls c] (async/alts!! [sieve-receiver timeout-chan])]
            (if (= c sieve-receiver)
              (when urls
                (log/debug :distributor2 {:enqueued (count urls)})
                (sieve/enqueue*! the-sieve urls)
                (recur current-url url-count))
              (recur current-url url-count))))))
    (log/info :distributor :graceful-shutdown)))

;; TODO better naming
;; TODO make go block?
(defn spawn-sieve->bench-handler [config the-sieve the-bench release-chan {:keys [delay] :or {delay 2000} :as _opts}]
  (async/thread
    (thread-util/set-thread-name (str the-ns-name "-sieve->bench"))
    (thread-util/set-thread-priority Thread/MAX_PRIORITY)
    (loop []
      (when-not (:ramper/stop @config)
        ;; TODO test with timeout and cond-let
        (if-let [[url next-fetch] (async/poll! release-chan)]
          (do
            (log/debug :sieve->bench-handler {:readd url})
            (workbench/readd the-bench url next-fetch))
          (when-let [url (sieve/dequeue! the-sieve)]
            (log/debug :sieve->bench-handler {:cons-bench url})
            (workbench/cons-bench the-bench url)))
        (recur)))
    (log/info :sieve->bench-handler :graceful-shutdown)))

(comment
  (do
    (require '[clojure.java.io :as io])
    (require '[ramper.util :as util])
    (require '[ramper.sieve.memory-sieve :as mem-sieve])

    (def urls (util/read-urls (io/file (io/resource "seed.txt")))))

  (do
    (def the-sieve (mem-sieve/memory-sieve))
    (def the-config (atom {:ramper/stop false}))
    (def the-bench (workbench/simple-bench-factory))
    (def sieve-receiver (async/chan 100))
    (def sieve-emitter (async/chan 100))
    (def releach-chan (async/chan 100)))

  (async/put! sieve-receiver (take 3 urls))

  (do
    (spawn-sieve->bench-handler the-config the-sieve the-bench releach-chan {})
    (spawn-distributor the-sieve the-bench sieve-receiver sieve-emitter 4))


  (async/put! sieve-receiver ["foo"])

  (async/poll! sieve-emitter)

  (do
    (swap! the-config assoc :ramper/stop true)
    (async/close! sieve-receiver)
    (async/close! sieve-emitter))

  (def c1 (async/chan))
  (async/close! c1)

  (async/alts!! [c1 (async/timeout 1000)])

  )
