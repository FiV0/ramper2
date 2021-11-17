(ns ramper.instance
  "The main entrypoint for creating a ramper instance."
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.sieve :as sieve :refer [FlushingSieve]]
            [ramper.sieve.memory-sieve]
            [ramper.sieve.mercator-sieve.wrapped]
            [ramper.store :as store]
            [ramper.store.parallel-buffered-store]
            [ramper.store.simple-store]
            [ramper.util :as util]
            [ramper.util.async :as async-util]
            [ramper.workbench :as workbench]
            [ramper.workbench.simple-bench.wrapped]
            [ramper.workbench.virtualized-bench.wrapped]
            [ramper.worker.distributor :as distributor]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.parser :as parser])
  (:import (java.io Closeable)))

(def config (atom {}))

;; TODO refactor cleanup parts from here and stop
(defn- end-loop
  "Takes an `instance-config` as returned by start and listens to whether some stopping criteria
  as been reached (e.g. max-urls). Closes opened resources. Returns a channel that will return
  the total time taken by the crawl in ms."
  [{:keys [parsers fetchers store sieve workbench start-time
           sieve-receiver resp-chan release-chan] :as _instance-config}]
  (async/go
    (loop [[fetcher & fetchers] fetchers]
      (when fetcher
        (async/<! fetcher)
        (recur fetchers)))
    ;; TODO adapt to timeout so that parsers won't get stopped early
    (log/info :end-loop :fetchers-closed)
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
    (log/info :end-loop :parsers-closed)
    (.close store)
    (when (instance? Closeable sieve)
      (.close sieve))
    (when (instance? Closeable workbench)
      (.close workbench))
    (let [time-ms (- (System/currentTimeMillis) start-time)]
      (log/info :end-time {:time (with-out-str (util/print-time time-ms))
                           :time-ms time-ms})
      time-ms)))

(defn- extra-info-printing [{:keys [workbench] :as _agent-config}]
  (distributor/print-bench-size-loop config workbench))

(defn start
  "Starts a ramper instance with the specified `seed-file`, `store-dir` and options. Returns
  an instance config map.

  `seed-file` - should be a text-file with line separated urls with which to intialize the crawl.
  `store-dir` - a directory where the crawled data should be stored.

  The options map specifies may specify the following parameters:

  :max-url - maximal number of urls to crawl
  :nb-fetchers - number of fetching go subroutines to spawn
  :nb-parsers - number of parsers go subroutines to spawn (must be below number of cores)
  :sieve-type - :memory | :mercator
  :store-type - :simple | :parallel (:simple is mostly for debugging purposes)
  :bench-type - :memory | :virtualized (:memory for small crawls)
  :extra-into - boolean to indicated whether some extra statistics should be logged."
  [seed-file store-dir
   {:keys [max-urls nb-fetchers nb-parsers sieve-type store-type bench-type extra-info fetch-filter]
    :or {nb-fetchers 32 nb-parsers 10 sieve-type :memory store-type :parallel bench-type :memory
         extra-info false}}]
  (when (<= (async-util/get-async-pool-size) nb-parsers)
    (throw (IllegalArgumentException. "Number of parsers must be below `core.async` thread pool size!")))
  (let [urls (cond->> (util/read-urls seed-file)
               fetch-filter (filter fetch-filter))
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 10)
        sieve-emitter (async/chan 10)
        release-chan (async/chan 10)
        the-sieve (sieve/create-sieve sieve-type)
        the-bench (workbench/create-workbench bench-type)
        the-store (apply store/create-store store-type store-dir
                         (cond-> []
                           (= store-type :parallel) (conj (* 2 (util/number-of-cores)))))]
    (sieve/enqueue*! the-sieve urls)
    (swap! config assoc :ramper/stop false)
    (let [fetchers (repeatedly nb-fetchers #(fetcher/spawn-fetcher sieve-emitter resp-chan release-chan {}))
          parsers (repeatedly nb-parsers #(parser/spawn-parser sieve-receiver resp-chan the-store
                                                               {:fetch-filter fetch-filter}))
          sieve-receiver-loop (distributor/spawn-sieve-receiver-loop the-sieve sieve-receiver)
          sieve-emitter-loop (distributor/spawn-sieve-emitter-loop config the-bench sieve-emitter max-urls)
          readd-loop (distributor/spawn-readd-loop the-bench release-chan)
          sieve-dequeue-loop (distributor/spawn-sieve-dequeue-loop config the-sieve the-bench)
          instance-config {:config config
                           :resp-chan resp-chan :sieve-receiver sieve-receiver
                           :sieve-emitter sieve-emitter :release-chan release-chan
                           :sieve the-sieve :workbench the-bench
                           :store the-store
                           :fetchers (doall fetchers) :parsers (doall parsers)
                           :sieve-receiver-loop sieve-receiver-loop :sieve-emitter-loop sieve-emitter-loop
                           :readd-loop readd-loop :sieve-dequeue-loop sieve-dequeue-loop
                           :start-time (System/currentTimeMillis)}]
      (when extra-info
        (extra-info-printing instance-config))
      (cond-> instance-config
        max-urls (assoc :time-chan (end-loop instance-config))))))

(defn stop
  "Stops a instance. Takes a `instance-config` as argument (see start)."
  [{:keys [sieve workbench resp-chan sieve-receiver sieve-emitter
           release-chan store parsers start-time] :as instance-config}]
  (when (satisfies? FlushingSieve sieve)
    (sieve/flush! sieve))
  (swap! config assoc :ramper/stop true)
  (async/close! resp-chan)
  (async/close! sieve-receiver)
  (async/close! sieve-emitter)
  (async/into [] sieve-receiver)
  (async/close! release-chan)
  ;; parsers should only close after fetchers have closed
  (run! async/<!! parsers)
  (.close store)
  (when (instance? Closeable sieve)
    (.close sieve))
  (when (instance? Closeable workbench)
    (.close workbench))
  (let [time-ms (- (System/currentTimeMillis) start-time)]
    (log/info :stop {:time (with-out-str (util/print-time time-ms))
                     :time-ms time-ms})
    (assoc instance-config :time-ms time-ms)))

(comment
  (require '[clojure.java.io :as io]
           '[ramper.customization :as custom])

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {}))

  (defn clojure-url? [url]
    (clojure.string/index-of url "clojure"))

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 10000 :nb-fetchers 5 :nb-parsers 2
                                                                              :extra-info true
                                                                              ;; :fetch-filter custom/https-filter?
                                                                              #_(every-pred custom/https-filter clojure-url?)
                                                                              ;; :sieve-type :mercator
                                                                              #_#_:bench-type :virtualized}))
  ;; sieve bench time
  ;; mem   mem   1min13sec
  ;; mer   mem
  ;; mem   vir   1min13sec
  ;; mer   vir


  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 10000 :nb-fetchers 2
                                                                              :nb-parsers 1 :sieve-type :mercator}))

  (do (stop s-map) nil)

  (async/<!! (:time-chan s-map))

  )
