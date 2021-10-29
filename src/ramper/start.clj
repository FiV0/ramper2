(ns ramper.start
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [ramper.util :as util]
            [ramper.sieve :as sieve]
            [ramper.store.parallel-buffered-store :as store]
            [ramper.worker.parser :as parser]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.distributor :as distributor]))

(def config (atom {}))

(defn start [seed-path store-dir]
  (let [urls (take 20 (util/read-urls seed-path))
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 100)
        nb-fetchers 32
        nb-parsers 5
        the-sieve (atom (sieve/sieve))
        the-store (store/parallel-buffered-store store-dir)]
    (swap! the-sieve sieve/add* urls)
    (swap! config assoc :ramper/stop false)
    (dotimes [_ nb-fetchers]
      (fetcher/spawn-fetcher config the-sieve resp-chan))
    (dotimes [_ nb-parsers]
      (parser/spawn-parser sieve-receiver resp-chan the-store))
    (distributor/spawn-distributor the-sieve sieve-receiver)
    {:config config :resp-chan resp-chan
     :sieve-receiver sieve-receiver :store the-store}))

(defn stop [{:keys [resp-chan sieve-receiver store]}]
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan)
  (async/close! sieve-receiver)
  (.close store))

(comment
  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir")))

  (stop s-map)

  )
