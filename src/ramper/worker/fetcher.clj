(ns ramper.worker.fetcher
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]
            [ramper.sieve :as sieve]
            [ramper.store :as store]))

(alter-var-root #'org.httpkit.client/*default-client* (fn [_] sni-client/default-client))

(defn spawn-fetcher [config the-sieve resp-chan]
  (async/go-loop []
    (if-not (:ramper/stop @config)
      (do
        (if-let [url (sieve/dequeue! the-sieve)]
          (do
            (log/info :fetcher {:dequeued url})
            (http/get url {:follow-redirects false
                           #_#_:proxy-url "http://localhost:8080"}
                      (fn [{:keys [error] :as resp}]
                        (if error
                          (log/error :fetcher {:type (type error)})
                          (async/>!! resp-chan resp)))))
          (async/<! (async/timeout 100)))
        (recur))
      (log/info :fetcher :graceful-shutdown))))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])
  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def the-sieve (atom (sieve/sieve)))
  (run! #(swap! the-sieve sieve/add %) (take 10 urls))
  (def the-config (atom {:ramper/stop false}))
  (def resp-chan (async/chan 100))

  (spawn-fetcher the-config the-sieve resp-chan)
  (swap! the-config assoc :ramper/stop true)

  (async/<!! resp-chan)

  )
