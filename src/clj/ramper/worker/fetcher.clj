(ns ramper.worker.fetcher
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]
            [ramper.util.async :as async-util]
            [ramper.util.threadpool :as threadpool]))

(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

;; TODO check if a future should be added to the async call in the callback
;; TODO how to handle too much backpressure in callbacks?
;; offer!
;; TODO add purge conditions in case of errors

(def pool (threadpool/create-threadpool 4 8))

(defn spawn-fetcher [sieve-emitter resp-chan release-chan {:keys [delay] :or {delay 2000}}]
  (async/go-loop []
    (if-let [url (async/<! sieve-emitter)]
      (do
        (log/debug :fetcher {:dequeued url})
        (http/get url {:follow-redirects false :timeout 2000
                       :proxy-url "http://localhost:8080"
                       :worker-pool pool}
                  (fn [{:keys [error] :as resp}]
                    (if error
                      (log/error :fetcher-callback {:error-type (type error)})
                      (future (async-util/multi->!! [[resp-chan resp]
                                                     [release-chan [url (+ (System/currentTimeMillis) delay)]]])))))
        (recur))
      (log/info :fetcher :graceful-shutdown))))

(comment
  (do
    (require '[clojure.java.io :as io])
    (require '[ramper.util :as util])
    (def urls (util/read-urls (io/file (io/resource "seed.txt")))))

  (do
    (def sieve-emitter (async/chan 100))
    (async/onto-chan! sieve-emitter (take 5 urls) false)
    (def resp-chan (async/chan 100))
    (def release-chan (async/chan 100)))

  (spawn-fetcher sieve-emitter resp-chan release-chan {})

  (async/<!! resp-chan)
  (async/<!! release-chan)
  (async/close! sieve-emitter)
  (async/close! resp-chan)

  )
