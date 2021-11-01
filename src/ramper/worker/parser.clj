(ns ramper.worker.parser
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.store :as store]
            [ramper.store.parallel-buffered-store :as para-store]
            [ramper.html-parser :as html]
            [lambdaisland.uri :as uri]))

(defn spawn-parser [sieve-receiver resp-chan the-store]
  (async/go-loop []
    (if-let [resp (async/<! resp-chan)]
      (if (string? (:body resp))
        (let [urls (doall (html/create-new-urls (-> resp :opts :url) (html/html->links (:body resp))))]
          #_(store/store the-store (-> resp :opts :url uri/uri) resp)
          (log/debug :parser {:store (-> resp :opts :url)})
          (async/>! sieve-receiver (map str urls))
          (recur))
        (recur))
      (log/info :parser :graceful-shutdown))))

(comment
  (require '[clojure.java.io :as io])
  (require '[ramper.util :as util])
  (require '[org.httpkit.client :as client])
  (require '[clojure.spec.alpha :as s])
  (require '[ramper.store.simple-record :as simple-record])

  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def sieve-receiver (async/chan 100))
  (def the-store (para-store/parallel-buffered-store (util/temp-dir "parser-test")))
  (def resp-chan (async/chan 100))

  (future
    (doseq [url (take 5 (drop 20 urls))]
      (async/>!! resp-chan @(client/get url {:follow-redirects false :proxy-url "http://localhost:8080"}))))

  (spawn-parser sieve-receiver resp-chan the-store)

  (async/close! resp-chan)

  (async/poll! sieve-receiver)

  (def resp @(client/get (first urls) {:follow-redirects false}))

  (store/store the-store (-> resp :opts :url uri/uri) resp)

  (def resps (doall (for [url (take 10 urls)] @(client/get url {:follow-redirects false :timeout 2000}))))

  (doseq [resp (remove :error resps)]
    ;; (println (-> resp :opts :url))
    ;; (println (s/valid? :store/record resp))
    (store/store the-store (-> resp :opts :url uri/uri) resp))

  )
