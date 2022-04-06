(ns ramper.worker.distributor
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.sieve :as sieve :refer [FlushingSieve]]
            [ramper.util :as util]
            [ramper.workbench :as workbench]))

(defn- instance-url? [url i n]
  (= i (util/url->bucket url n)))

(defn spawn-sieve-receiver-loop [the-sieve sieve-receiver {:keys [external-chan i n]}]
  (async/go-loop []
    (if-let [urls (async/<! sieve-receiver)]
      (let [[instance-urls other-urls] (util/seperate #(instance-url? % i n) urls)]
        (when (seq instance-urls)
          (sieve/enqueue*! the-sieve instance-urls))
        ;; TODO? check on external-chan
        (when (seq other-urls)
          (async/>! external-chan other-urls))
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
        ;; TODO experiment with timeout here
        (recur url-count)))))

(defn spawn-readd-loop [the-bench release-chan]
  (async/go-loop []
    (if-let [[url next-fetch] (async/<! release-chan)]
      (do
        (workbench/readd! the-bench url next-fetch)
        (recur))
      (log/info :readd-loop :graceful-shutdown))))

;; TODO maybe add dequeue! in channel
(defn spawn-sieve-dequeue-loop [config the-sieve the-bench {:keys [schedule-filter]}]
  (let [flushing-sieve (satisfies? FlushingSieve the-sieve)]
    (async/go-loop []
      (if-not (:ramper/stop @config)
        (do
          (if-let [url (sieve/dequeue! the-sieve)]
            (if schedule-filter
              (when (schedule-filter url)
                (workbench/cons-bench! the-bench url))
              (workbench/cons-bench! the-bench url))
            (when flushing-sieve
              (sieve/flush! the-sieve)
              ;; as the sieve is blocked when flushing this somehow
              ;; assures that flushes don't happen too often
              (async/<! (async/timeout 100))))
          (recur))
        (log/info :sieve-dequeue-loop :graceful-shutdown)))))

(defn print-bench-size-loop [config the-bench]
  (async/go-loop []
    (if-not (:ramper/stop @config)
      (do
        (log/info :print-bench-size-loop {:size (count the-bench)})
        (async/<! (async/timeout 1000))
        (recur))
      (log/info :print-bench-size-loop :graceful-shutdown))))

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
