(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util.thread :as thread-util]
            [ramper.sieve :as sieve :refer [FlushingSieve]]
            [ramper.workbench :as workbench]))

(defn spawn-distributor [the-sieve the-bench sieve-receiver sieve-emitter max-urls]
  (async/thread
    (thread-util/set-thread-name (str (namespace ::_)))
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

(defn spawn-sieve-receiver-loop [the-sieve sieve-receiver]
  (async/go-loop []
    (if-let [urls (async/<! sieve-receiver)]
      (do
        (sieve/enqueue*! the-sieve urls)
        (recur))
      (log/info :sieve-receiver-loop :graceful-shutdown))))

;; TODO can we get rid of the config stop checking?
(defn spawn-sieve-emitter-loop [config the-bench sieve-emitter max-urls]
  (async/go-loop [url-count 0]
    (if (or (= url-count max-urls) (:ramper/stop @config))
      (do
        (async/close! sieve-emitter)
        (log/info :sieve-emitter-loop :graceful-shutdown))
      (if-let [url (workbench/dequeue! the-bench)]
        (if (async/>! sieve-emitter url)
          (recur (inc url-count))
          (log/info :sieve-emitter-loop :graceful-shutdown))
        (recur url-count)))))


;; TODO better naming
;; TODO make go block?
(defn spawn-sieve->bench-handler [config the-sieve the-bench release-chan {:keys [delay] :or {delay 2000} :as _opts}]
  (async/go-loop []
    (if-not (:ramper/stop @config)
      ;; TODO test with timeout and cond-let
      (do
        (if-let [[url next-fetch] (async/poll! release-chan)]
          (do
            (log/debug :sieve->bench-handler {:readd url})
            (workbench/readd! the-bench url next-fetch))
          (when-let [url (sieve/dequeue! the-sieve)]
            (log/debug :sieve->bench-handler {:cons-bench url})
            (workbench/cons-bench! the-bench url)))
        (recur))
      (log/info :sieve->bench-handler :graceful-shutdown))))


(defn spawn-readd-loop [the-bench release-chan]
  (async/go-loop []
    (if-let [[url next-fetch] (async/<! release-chan)]
      (do
        (workbench/readd! the-bench url next-fetch)
        (recur))
      (log/info :readd-loop :graceful-shutdown))))

;; TODO maybe add dequeue! in channel
(defn spawn-sieve-dequeue-loop [config the-sieve the-bench]
  (let [flushing-sieve (satisfies? FlushingSieve the-sieve)]
    (async/go-loop []
      (if-not (:ramper/stop @config)
        (do
          (if-let [url (sieve/dequeue! the-sieve)]
            (workbench/cons-bench! the-bench url)
            (when flushing-sieve
              (sieve/flush! the-sieve)))
          (recur))
        (log/info :sieve-dequeue-loop :graceful-shutdown)))))


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
