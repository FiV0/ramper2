(ns ramper.worker.fetcher
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]
            [ramper.sieve :as sieve]
            [ramper.store :as store]))

(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

;; TODO check if a future should be added to the async call in the callback

(defn spawn-fetcher [sieve-emitter resp-chan]
  (async/go-loop []
    (if-let [url (async/<! sieve-emitter)]
      (do
        (log/debug :fetcher {:dequeued url})
        (http/get url {:follow-redirects false #_#_:timeout 2000
                       #_#_:proxy-url "http://localhost:8080"}
                  (fn [{:keys [error] :as resp}]
                    (if error
                      (log/error :fetcher {:error-type (type error)})
                      (future (async/>!! resp-chan resp)))))
        (recur))
      (log/info :fetcher :graceful-shutdown))))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])
  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def sieve-emitter (async/chan 100))
  (async/onto-chan! sieve-emitter (take 5 urls) false)
  (def resp-chan (async/chan 100))

  (spawn-fetcher sieve-emitter resp-chan)

  (async/<!! resp-chan)
  (async/close! sieve-emitter)
  (async/close! resp-chan)

  )
