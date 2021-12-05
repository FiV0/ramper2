(ns ramper.worker.fetcher
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]
            [ramper.util.async :as async-util]
            [ramper.util.http :as http-util]
            [ramper.util.threadpool :as threadpool]))

(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

;; TODO check if a future should be added to the async call in the callback
;; TODO how to handle too much backpressure in callbacks?
;; offer!
;; TODO add purge conditions in case of errors

;; TODO investigate this http-kit redirect bug
(def default-http-opts {:follow-redirects false})

(defn- default-http-get [url resp-chan release-chan delay http-opts]
  (http/get url (assoc http-opts :timeout delay)
            (fn [{:keys [error status] :as resp}]
              (if (or error (not (http-util/successful-response? status)))
                (do
                  (log/debug :fetcher-callback (cond-> {}
                                                 error (assoc :error-type (type error))
                                                 status (assoc :status-code status)))
                  (future (async/>!! release-chan [url (+ (System/currentTimeMillis) delay)])))
                (future (async-util/multi->!!
                         [[resp-chan resp]
                          [release-chan [url (+ (System/currentTimeMillis) delay)]]]))))))

(defn spawn-fetcher [sieve-emitter resp-chan release-chan
                     {:keys [delay http-get http-opts] :or {delay 2000 http-get default-http-get}}]
  (async/go-loop []
    (if-let [url (async/<! sieve-emitter)]
      (do
        (log/debug :fetcher {:dequeued url})
        (http-get url resp-chan release-chan delay http-opts)
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
