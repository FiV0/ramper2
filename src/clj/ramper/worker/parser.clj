(ns ramper.worker.parser
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.store :as store]
            [ramper.store.simple-record :as simple-record]
            [ramper.html-parser :as html]
            [lambdaisland.uri :as uri]))

(defn link-extraction [origin-url body]
  (->> (html/create-new-urls origin-url (html/html->links body))
       (map str)))

(defn default-store-fn [resp the-store fetch-filter]
  (when (string? (:body resp))
    (let [origin-url (-> resp :opts :url)
          urls (doall (cond->> (link-extraction origin-url (:body resp))
                        fetch-filter (filter fetch-filter)))]
      (store/store the-store (simple-record/simple-record (uri/uri origin-url) resp))
      (log/debug :parser {:store origin-url})
      urls)))

(defn spawn-parser [sieve-receiver resp-chan the-store
                    {:keys [fetch-filter store-fn] :or {store-fn default-store-fn}}]
  (async/go-loop []
    (if-let [resp (async/<! resp-chan)]
      (if-let [urls (store-fn resp the-store fetch-filter)]
        (do
          (async/>! sieve-receiver urls)
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
