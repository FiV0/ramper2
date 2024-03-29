(ns ramper.worker.parser
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.parser :as parser]
            [ramper.parser.html :as html]
            [ramper.parser.text]
            [ramper.parser.xml]
            [ramper.store :as store]
            [ramper.store.simple-record :as simple-record]
            [ramper.url :as url]
            [ramper.util.robots-txt :as robots-txt]
            [ramper.util.robots-store.wrapped :as robots-store]))

(defn robots-txt? [content-type url]
  (and (= content-type "text/plain")
       (url/robots-txt? url)))

(defn sitemap? [content-type url]
  (and (= content-type "application/xml")
       (url/sitemap? url)))

(defn link-extraction [origin-url parsed robots-store]
  (let [urls (->> (html/create-new-urls origin-url (parser/extract-links parsed))
                  (map str))]
    (cond->> urls
      robots-store (remove #(robots-store/disallowed? robots-store %)))))

(defn try-parse [content-type body]
  (try
    (parser/parse content-type body)
    (catch RuntimeException _e
      nil)))

;; TODO think about how to best handle complexity added by special cases
(defn default-parse-fn [resp the-store fetch-filter store-filter follow-filter robots-store]
  (when-let [{:keys [content-type parsed-content] :as parsed} (try-parse (-> resp :headers :content-type) (:body resp))]
    (let [origin-url (-> resp :opts :url)
          is-robots-txt (robots-txt? content-type origin-url)
          ;; TODO (str/starts-with? (-> resp :headers :content-type) "text/html")
          urls (when (or (not follow-filter) (follow-filter resp))
                 (seq (doall (cond->> (link-extraction origin-url parsed robots-store)
                               fetch-filter (filter fetch-filter)))))]
      (when (or (not store-filter) (store-filter resp))
        (store/store the-store (simple-record/simple-record (uri/uri origin-url) resp)))
      (when (and robots-store is-robots-txt)
        (robots-store/add-robots-txt! robots-store (url/base origin-url) (robots-txt/parse-robots-txt parsed-content)))
      (log/debug :parser {:store origin-url})
      urls)))

(defn spawn-parser [sieve-receiver resp-chan the-store
                    {:keys [fetch-filter store-filter follow-filter parse-fn robots-store]
                     :or {parse-fn default-parse-fn}}]
  (async/go-loop []
    (if-let [resp (async/<! resp-chan)]
      (if-let [urls (parse-fn resp the-store fetch-filter store-filter follow-filter robots-store)]
        (do
          (async/>! sieve-receiver urls)
          (recur))
        (recur))
      (log/info :parser :graceful-shutdown))))

(comment
  (require '[clojure.java.io :as io]
           '[ramper.util :as util]
           '[org.httpkit.client :as client]
           '[clojure.spec.alpha :as s]
           '[ramper.store.simple-record :as simple-record]
           '[ramper.store.parallel-buffered-store :as para-store])

  (def urls (util/read-urls (io/file (io/resource "seed.txt"))))

  (def sieve-receiver (async/chan 100))
  (def the-store (para-store/parallel-buffered-store (util/temp-dir "parser-test")))
  (def resp-chan (async/chan 100))

  (future
    (doseq [url (take 5 (drop 20 urls))]
      (async/>!! resp-chan @(client/get url {:follow-redirects false :proxy-url "http://localhost:8080"}))))

  (spawn-parser sieve-receiver resp-chan the-store {})

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
