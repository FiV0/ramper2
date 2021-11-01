(ns ramper.start
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.util :as util]
            [ramper.sieve :as sieve]
            [ramper.store.parallel-buffered-store :as store]
            [ramper.worker.parser :as parser]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.distributor :as distributor]
            [ramper.sieve.memory-sieve :as mem-sieve]))

(def config (atom {}))

(defn end-time [{:keys [parsers fetchers store start-time sieve-receiver resp-chan] :as _agent-config}]
  (async/go
    (loop [[fetcher & fetchers] fetchers]
      (when fetcher
        (async/<! fetcher)
        (recur fetchers)))
    ;; TODO adapt to timeout
    (log/info :end-time :fetchers-closed)
    (async/<! (async/timeout 1000))
    (async/close! resp-chan)
    (async/close! sieve-receiver)
    (async/into [] sieve-receiver)
    (loop [[parser & parsers] parsers]
      (when parser
        (async/<! parser)
        (recur parsers)))
    (log/info :end-time :parsers-closed)
    (.close store)
    (let [time-ms (- (System/currentTimeMillis) start-time)]
      (log/info :end-time {:time (with-out-str (util/print-time time-ms))
                           :time-ms time-ms})
      time-ms)))

(defn start [seed-path store-dir {:keys [max-url nb-fetchers nb-parsers] :or {nb-fetchers 32 nb-parsers 10}}]
  (let [urls (util/read-urls seed-path)
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 10)
        sieve-emitter (async/chan 10)
        the-sieve (mem-sieve/memory-sieve)
        the-store (store/parallel-buffered-store store-dir)
        counter (atom 0)]
    (sieve/enqueue*! the-sieve urls)
    (swap! config assoc :ramper/stop false)
    (let [fetchers (repeatedly nb-fetchers #(fetcher/spawn-fetcher sieve-emitter resp-chan))
          parsers (repeatedly nb-parsers #(parser/spawn-parser sieve-receiver resp-chan the-store))
          distributor (distributor/spawn-distributor the-sieve sieve-receiver sieve-emitter max-url)
          agent-config {:config config :resp-chan resp-chan
                        :sieve-receiver sieve-receiver :sieve-emitter sieve-emitter
                        :store the-store
                        :fetchers (doall fetchers) :parsers (doall parsers)
                        :distributor distributor :start-time (System/currentTimeMillis)
                        :counter counter}]
      (cond-> agent-config
        max-url (assoc :time-chan (end-time agent-config))))))

(defn stop [{:keys [resp-chan sieve-receiver sieve-emitter store parsers fetchers start-time] :as agent-config}]
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan)
  (async/close! sieve-receiver)
  (async/close! sieve-emitter)
  (async/into [] sieve-receiver)
  ;; (run! async/<!! fetchers)
  (run! async/<!! parsers)
  (.close store)
  (let [time-ms (- (System/currentTimeMillis) start-time)]
    (log/info :end-time {:time (with-out-str (util/print-time time-ms))
                         :time-ms time-ms})
    (assoc agent-config :time-ms time-ms)))

(defn calc-time [{:keys [start-time end-time-chan]}]
  (- (async/<!! end-time-chan) start-time))

(comment
  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {}))
  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-url 1000}))
  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-url 100 :nb-fetchers 5 :nb-parsers 2}))

  (async/<!! (:sieve-receiver s-map))

  (async/into )

  (-> s-map :counter deref)

  (stop s-map)

  (calc-time s-map)



  )
