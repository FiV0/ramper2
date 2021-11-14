(ns ramper.start
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [ramper.sieve :as sieve :refer [FlushingSieve]]
            [ramper.sieve.memory-sieve :as mem-sieve]
            [ramper.sieve.mercator-sieve.wrapped :as mer-sieve]
            [ramper.store.parallel-buffered-store :as parallel-store]
            [ramper.store.simple-store :as simple-store]
            [ramper.util :as util]
            [ramper.util.async :as async-util]
            [ramper.workbench.simple-bench.wrapped :as simple-bench]
            [ramper.workbench.virtualized-bench.wrapped :as virtual-bench]
            [ramper.worker.distributor :as distributor]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.parser :as parser])
  (:import (java.io Closeable)))

(def config (atom {}))

(defn end-time [{:keys [parsers fetchers store start-time sieve-receiver resp-chan release-chan] :as agent-config}]
  (async/go
    (loop [[fetcher & fetchers] fetchers]
      (when fetcher
        (async/<! fetcher)
        (recur fetchers)))
    ;; TODO adapt to timeout
    (log/info :end-time :fetchers-closed)
    (swap! config assoc :ramper/stop true)
    (async/<! (async/timeout 1000))
    (async/close! resp-chan)
    (async/close! sieve-receiver)
    (async/into [] sieve-receiver)
    (async/close! release-chan)
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

(defn start [seed-path store-dir
             {:keys [max-url nb-fetchers nb-parsers sieve-type store-type bench-type]
              :or {nb-fetchers 32 nb-parsers 10 sieve-type :memory store-type :parallel bench-type :memory}}]
  (when (<= (async-util/get-async-pool-size) nb-parsers)
    (throw (IllegalArgumentException. "Number of parsers must be below `core.async` thread pool size!")))
  (let [urls (util/read-urls seed-path)
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 10)
        sieve-emitter (async/chan 10)
        release-chan (async/chan 10)
        the-sieve (case sieve-type
                    :memory (mem-sieve/memory-sieve)
                    :mercator (mer-sieve/mercator-sieve)
                    (throw (IllegalArgumentException. (str "No such sieve: " sieve-type))))
        the-bench (case bench-type
                    :memory (simple-bench/simple-bench-factory)
                    :virtualized (virtual-bench/virtualized-bench-factory)
                    (throw (IllegalArgumentException. (str "No such workbench: " bench-type))))
        the-store (case store-type
                    :simple (simple-store/simple-store store-dir)
                    :parallel (parallel-store/parallel-buffered-store store-dir (* 2 (util/number-of-cores)))
                    (throw (IllegalArgumentException. (str "No such store: " store-type))))]
    (sieve/enqueue*! the-sieve urls)
    (swap! config assoc :ramper/stop false)
    (let [fetchers (repeatedly nb-fetchers #(fetcher/spawn-fetcher sieve-emitter resp-chan release-chan {}))
          parsers (repeatedly nb-parsers #(parser/spawn-parser sieve-receiver resp-chan the-store))
          ;; distributor (distributor/spawn-distributor the-sieve the-bench sieve-receiver sieve-emitter max-url)
          ;; sieve->bench-loops (repeatedly nb-sieve->bench
          ;;                                #(distributor/spawn-sieve->bench-handler config the-sieve the-bench release-chan {}))
          sieve-receiver-loop (distributor/spawn-sieve-receiver-loop the-sieve sieve-receiver)
          sieve-emitter-loop (distributor/spawn-sieve-emitter-loop config the-bench sieve-emitter max-url)
          readd-loop (distributor/spawn-readd-loop the-bench release-chan)
          sieve-dequeue-loop (distributor/spawn-sieve-dequeue-loop config the-sieve the-bench)
          agent-config {:config config
                        :resp-chan resp-chan :sieve-receiver sieve-receiver
                        :sieve-emitter sieve-emitter :release-chan release-chan
                        :sieve the-sieve :workbench the-bench
                        :store the-store
                        :fetchers (doall fetchers) :parsers (doall parsers)
                        ;; :distributor distributor
                        ;; :sieve->bench-loops (doall sieve->bench-loops)
                        :sieve-receiver-loop sieve-receiver-loop :sieve-emitter-loop sieve-emitter-loop
                        :readd-loop readd-loop :sieve-dequeue-loop sieve-dequeue-loop
                        :start-time (System/currentTimeMillis)}]
      (cond-> agent-config
        max-url (assoc :time-chan (end-time agent-config))))))

(defn stop [{:keys [sieve workbench resp-chan sieve-receiver sieve-emitter
                    release-chan store parsers fetchers start-time] :as agent-config}]
  (when (satisfies? FlushingSieve sieve)
    (sieve/flush! sieve))
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan)
  (async/close! sieve-receiver)
  (async/close! sieve-emitter)
  (async/into [] sieve-receiver)
  (async/close! release-chan)
  ;; (run! async/<!! fetchers)
  (run! async/<!! parsers)
  (.close store)
  (when (instance? Closeable sieve)
    (.close sieve))
  (when (instance? Closeable workbench)
    (.close workbench))
  (let [time-ms (- (System/currentTimeMillis) start-time)]
    (log/info :stop {:time (with-out-str (util/print-time time-ms))
                     :time-ms time-ms})
    (assoc agent-config :time-ms time-ms)))

(comment

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {}))


  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-url 100000 #_#_:sieve-type :mercator
                                                                              :bench-type :virtualized}))
  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-url 100000 :nb-fetchers 5 :nb-parsers 2
                                                                              #_#_:sieve-type :mercator
                                                                              #_#_:bench-type :virtualized}))
  ;; sieve bench time
  ;; mem   mem   1min13sec
  ;; mer   mem
  ;; mem   vir   1min13sec
  ;; mer   vir


  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-url 10000 :nb-fetchers 2
                                                                              :nb-parsers 1 :sieve-type :mercator}))

  (do (stop s-map) nil)

  (-> s-map :workbench deref :delay-queue first second :next-fetch (- (System/currentTimeMillis)) )
  (-> s-map :workbench workbench/peek-bench )
  (-> s-map :sieve :sieve-atom deref :new count)
  (-> s-map :release-chan (async/poll!))
  (-> s-map :sieve-receiver (async/poll!))
  (-> s-map :sieve-emitter (async/close!))

  (async/<!! (:time-chan s-map))

  )
