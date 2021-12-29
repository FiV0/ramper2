(ns repl-sessions.http-client
  (:require [aleph.http :as aleph]
            [aleph.http.core]
            [aleph.netty :as netty]
            [byte-streams :as bs]
            [clj-http.client :as clj-http]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [manifold.deferred :as d]
            [org.httpkit.client :as httpkit]
            [ramper.instance :as instance]
            [ramper.store :as store]
            [ramper.url :as url]
            [ramper.util.async :as async-util]
            [ramper.util.byte-serializer :as byte-serializer]
            [ramper.worker.parser :as parser]
            [taoensso.nippy :as nippy]))

(def url "https://finnvolkel.com")
(def url-404 "https://finnvolkel.com/about/adfafsa")
(def url-robots-txt "https://finnvolkel.com/robots.txt")

(comment
  @(aleph/get url-404 {:throw-exceptions false})


  @(httpkit/get url-404 {:follow-redirects false :timeout 3000
                         #_#_:proxy-url "http://localhost:8080"})

  @(httpkit/get url-robots-txt {:follow-redirects false :timeout 3000
                                #_#_:proxy-url "http://localhost:8080"})


  (clj-http/get url {:proxy-host "localhost" :proxy-port 1234})

  (dotimes [_ 10000]
    (d/on-realized (aleph/get "http://localhost:8080" {:throw-exceptions false})
                   (fn [r] (println "Status: " (:status r)))
                   (fn [e] (println "Error type: " (type e)))))

  )

(def aleph-pool (aleph/connection-pool {:connection-options
                                        {:proxy-options {:host "localhost"
                                                         :port 8080}
                                         ;; :ssl-context (netty/self-signed-ssl-context)
                                         }}))

(comment

  (d/on-realized (aleph/get url-404 {:pool aleph-pool
                                     :throw-exceptions false})
                 (fn [r] (println "Status: " (:status r)))
                 (fn [e] (println "Error type: " (type e))))

  (d/on-realized (aleph/get "http://localhost:8080" {:pool aleph-pool})
                 (fn [r] (println "Status: " (:status r)))
                 (fn [e] (println "Error type: " (type e))))


  (def res (-> @(aleph/get "http://localhost:8080" {:pool aleph-pool})
               (update :body bs/to-string)
               (assoc :url "http://localhost:8080")))


  )

(nippy/extend-freeze aleph.http.core.NettyResponse :netty-response/serialize
                     [this os]
                     (byte-serializer/write-array os (-> this
                                                         (select-keys  [:url :status :body])
                                                         nippy/freeze)))

(nippy/extend-thaw :netty-repsonse/serialize
                   [is]
                   (-> is byte-serializer/read-array nippy/thaw))

(defn aleph-http-get [url resp-chan release-chan delay]
  (let [get-url (str (url/https->http url))]
    (d/on-realized (aleph/get get-url {:pool aleph-pool :throw-exceptions false})
                   (fn [{:keys [error] :as resp}]
                     (if error
                       (log/warn :fetcher-callback {:error-type (type error)})
                       (future (async-util/multi->!!
                                [[resp-chan (assoc resp :url get-url)]
                                 [release-chan [url (+ (System/currentTimeMillis) delay)]]]))))
                   (fn [e]
                     (log/error :fetcher-callback {:ex-type (type e)})))))

(defn aleph-parse-fn [resp the-store fetch-filter]
  (let [url (:url resp)
        resp (update resp :body bs/to-string)
        urls (doall (cond->> (parser/link-extraction url (:body resp))
                      fetch-filter (filter fetch-filter)))]
    (store/store the-store resp)
    (log/debug :aleph-store-fn {:url url})
    urls))

(comment
  (def i-map (instance/start (io/file (io/resource "seed.txt"))
                             (io/file "store-dir")
                             {:max-urls 1000
                              ;; :nb-fetchers 1 :nb-parsers 1
                              :extra-info true
                              :http-get aleph-http-get
                              :parse-fn aleph-parse-fn}))

  (do (instance/stop i-map) nil)
  )
