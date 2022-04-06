(ns ramper.instance
  "The entrypoint for creating a ramper instance."
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [ramper.sieve :as sieve :refer [FlushingSieve]]
            [ramper.sieve.memory-sieve]
            [ramper.sieve.mercator-sieve.wrapped]
            [ramper.store :as store]
            [ramper.store.parallel-buffered-store]
            [ramper.store.simple-store]
            [ramper.util :as util]
            [ramper.util.async :as async-util]
            [ramper.util.nippy-extensions]
            [ramper.util.robots-store.wrapped :as robots-txt]
            [ramper.workbench :as workbench]
            [ramper.workbench.simple-bench.wrapped]
            [ramper.workbench.virtualized-bench.wrapped]
            [ramper.worker.distributor :as distributor]
            [ramper.worker.fetcher :as fetcher]
            [ramper.worker.parser :as parser]
            [taoensso.nippy :as nippy])
  (:import (java.io Closeable)
           (ramper.sieve.memory_sieve MemorySieve)))

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

(def ^:private pause-dir-name "pause-dir")
(def ^:private sieve-file "sieve.nippy")
(def ^:private workbench-file "workbench.nippy")
(def ^:private robots-store-file "robots_store.nippy")

(defn- pause-directory [store-dir]
  (io/file store-dir pause-dir-name))

(defn- freeze-sieve [sieve pause-dir]
  ;; TODO make this universal, not dependent on type
  (when (instance? MemorySieve sieve)
    (nippy/freeze-to-file (io/file pause-dir sieve-file) sieve)))

(defn- thaw-sieve [pause-dir]
  (nippy/thaw-from-file (io/file pause-dir sieve-file)))

(defn- freeze-workbench [bench pause-dir]
  (nippy/freeze-to-file (io/file pause-dir workbench-file) bench))

(defn- thaw-workbench [pause-dir]
  (nippy/thaw-from-file (io/file pause-dir workbench-file)))

(defn- freeze-robots-store [robots-store pause-dir]
  (nippy/freeze-to-file (io/file pause-dir robots-store-file) @robots-store))

(defn- thaw-robots-store [pause-dir]
  (atom (nippy/thaw-from-file (io/file pause-dir robots-store-file))))

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
  :extra-into - boolean to indicated whether some extra statistics should be logged.
  :fetch-filter - a predicate applied to urls before they are added to the sieve
  :schedule-filter - a predicate applied to urls before the resource gets fetched (after the sieve)
  :store-filter - a predicate applied to the response before it is stored
  :follow-filter - a predicate applied to the response before new links are extracted
  :http-get - the function retrieving new resources. TODO (improve doc for http-get)
  :parse-fn - TODO (might change)
  :new - whether this is a new crawl or starting from a paused one"
  [seed-file store-dir
   {:keys [max-urls nb-fetchers nb-parsers sieve-type store-type bench-type extra-info
           fetch-filter schedule-filter store-filter follow-filter http-get http-opts parse-fn
           robots-txt new]
    :or {nb-fetchers 32 nb-parsers 10 sieve-type :memory store-type :parallel bench-type :memory
         extra-info false robots-txt true new true}
    :as opts}]
  (when (<= (async-util/get-async-pool-size) nb-parsers)
    (throw (IllegalArgumentException. "Number of parsers must be below `core.async` thread pool size!")))
  (when-not (.exists store-dir)
    (log/info :instance/start (str "Creating store dir at: " store-dir))
    (.mkdirs store-dir))
  (let [urls (cond->> (util/read-urls seed-file)
               fetch-filter (filter fetch-filter))
        pause-dir (pause-directory store-dir)
        resp-chan (async/chan 100)
        sieve-receiver (async/chan 10)
        sieve-emitter (async/chan 10)
        release-chan (async/chan 10)
        the-sieve (if (and (not new) (= :memory sieve-type))
                    (thaw-sieve pause-dir)
                    (sieve/create-sieve sieve-type))
        the-bench (if-not new
                    (thaw-workbench pause-dir)
                    (workbench/create-workbench bench-type {:robots-txt robots-txt}))
        the-store (apply store/create-store store-type store-dir
                         (cond-> []
                           (= store-type :parallel) (conj (* 2 (util/number-of-cores)))))
        http-opts (merge fetcher/default-http-opts http-opts)
        the-robots-store (if-not new
                           (thaw-robots-store pause-dir)
                           (robots-txt/robots-txt-store))]
    (sieve/enqueue*! the-sieve urls)
    (swap! config assoc :ramper/stop false)
    (let [fetchers (repeatedly nb-fetchers #(fetcher/spawn-fetcher sieve-emitter resp-chan release-chan
                                                                   (cond-> (select-keys opts [:http-get])
                                                                     true (assoc :http-opts http-opts)
                                                                     robots-txt (assoc :robots-store the-robots-store))))
          parsers (repeatedly nb-parsers #(parser/spawn-parser sieve-receiver resp-chan the-store
                                                               (cond-> (select-keys opts [:fetch-filter :store-filter
                                                                                          :follow-filter :parse-fn])
                                                                 robots-txt (assoc :robots-store the-robots-store))))
          sieve-receiver-loop (distributor/spawn-sieve-receiver-loop the-sieve sieve-receiver)
          sieve-emitter-loop (distributor/spawn-sieve-emitter-loop config the-bench sieve-emitter max-urls)
          readd-loop (distributor/spawn-readd-loop the-bench release-chan)
          sieve-dequeue-loop (distributor/spawn-sieve-dequeue-loop config the-sieve the-bench
                                                                   (select-keys opts [:schedule-filter]))
          instance-config {:config config :store-dir store-dir
                           :opts opts
                           :resp-chan resp-chan :sieve-receiver sieve-receiver
                           :sieve-emitter sieve-emitter :release-chan release-chan
                           :sieve the-sieve :workbench the-bench
                           :store the-store :robots-store the-robots-store
                           :fetchers (doall fetchers) :parsers (doall parsers)
                           :sieve-receiver-loop sieve-receiver-loop :sieve-emitter-loop sieve-emitter-loop
                           :readd-loop readd-loop :sieve-dequeue-loop sieve-dequeue-loop
                           :start-time (System/currentTimeMillis)}]
      (when extra-info
        (extra-info-printing instance-config))
      (cond-> instance-config
        max-urls (assoc :time-chan (end-loop instance-config))))))

(defn stop
  "Stops an instance. Takes a `instance-config` as argument (see start)."
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

(defn pause
  "Pauses an instance. Takes a `instance-config` as argument (see start)."
  [{:keys [sieve workbench robots-store store-dir opts] :as instance-config}]
  (let [pause-dir (pause-directory store-dir)]
    (stop instance-config)
    (when-not (.exists pause-dir)
      (log/info :instance/pause (str "Creating pause dir at: " pause-dir))
      (.mkdirs pause-dir))
    (freeze-sieve sieve pause-dir)
    (freeze-workbench workbench pause-dir)
    (when (:robots-txt opts)
      (freeze-robots-store robots-store pause-dir))))
