(ns ramper.start
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [ramper.util :as util]
            [ramper.sieve :as sieve]
            [ramper.store.parallel-buffered-store :as store]
            [ramper.worker.parser :as parser]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.distributor :as distributor]
            [ramper.sieve.memory-sieve :as mem-sieve]))

(def config (atom {}))

(defn start [seed-path store-dir]
  (let [urls (util/read-urls seed-path)
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 100)
        sieve-emitter (async/chan 100)
        nb-fetchers 32
        nb-parsers 5
        the-sieve (mem-sieve/memory-sieve)
        the-store (store/parallel-buffered-store store-dir)]
    (sieve/enqueue*! the-sieve urls)
    (swap! config assoc :ramper/stop false)
    (let [fetchers (repeatedly nb-fetchers #(fetcher/spawn-fetcher config sieve-emitter resp-chan))
          parsers (repeatedly nb-parsers #(parser/spawn-parser sieve-receiver resp-chan the-store))
          distributor (distributor/spawn-distributor the-sieve sieve-receiver sieve-emitter)]
      {:config config :resp-chan resp-chan
       :sieve-receiver sieve-receiver :sieve-emitter sieve-emitter
       :store the-store
       :fetchers (doall fetchers) :parsers (doall parsers)
       :distributor distributor})))


(defn stop [{:keys [resp-chan sieve-receiver sieve-emitter store parsers]}]
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan)
  (async/close! sieve-receiver)
  (async/close! sieve-emitter)
  ;; TO check we no longer write to the store
  (run! async/<!! parsers)
  (.close store))

(comment
  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir")))

  (stop s-map)

  )
