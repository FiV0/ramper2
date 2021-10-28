(ns ramper.start
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            [ramper.util :as util]
            [ramper.sieve :as sieve]
            [ramper.store.parallel-buffered-store :as store]
            [ramper.worker.parser :as parser]
            [ramper.worker.fetcher :as fetcher]))

(def config (atom {}))

(defn start [seed-path store-dir]
  (let [urls (util/read-urls seed-path)
        resp-chan (async/chan 100)
        nb-fetchers 32
        nb-parsers 5
        the-sieve (atom (sieve/sieve))
        the-store (store/parallel-buffered-store store-dir)]
    (swap! the-sieve sieve/add* urls)
    (swap! config assoc :ramper/stop false)
    (dotimes [_ nb-fetchers]
      (fetcher/spawn-fetcher config the-sieve resp-chan))
    (dotimes [_ nb-parsers]
      (parser/spawn-parser the-sieve resp-chan the-store))
    {:config config :resp-chan resp-chan}))

(defn stop [{:keys [resp-chan]}]
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan))

(comment
  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir")))

  (stop s-map)

  )
