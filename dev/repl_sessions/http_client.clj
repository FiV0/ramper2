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
            [ramper.url :as url]
            [ramper.util.async :as async-util]
            [ramper.util.byte-serializer :as byte-serializer]
            [taoensso.nippy :as nippy]))

(def url "https://finnvolkel.com")
(def url-404 "https://finnvolkel.com/about/adfafsa")

(comment
  @(aleph/get url-404 {:throw-exceptions false})


  @(httpkit/get url-404 {:follow-redirects false :timeout 3000
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
               (update :body bs/to-string))))


(defn aleph-resp->http-kit-repsp [url resp]
  (assoc-in {:body (bs/to-string (:body resp))
             :headers (into {} (:headers resp))
             :status (:status resp)}
            [:opts :url] url))

(defn aleph-http-get [url resp-chan release-chan delay]
  (let [get-url (str (url/https->http url))]
    (d/on-realized (aleph/get get-url {:pool aleph-pool :throw-exceptions false})
                   (fn [{:keys [error] :as resp}]
                     (if error
                       (log/warn :fetcher-callback {:error-type (type error)})
                       (future (async-util/multi->!!
                                [[resp-chan (aleph-resp->http-kit-repsp get-url resp)]
                                 [release-chan [url (+ (System/currentTimeMillis) delay)]]]))))
                   (fn [e]
                     (log/error :fetcher-callback {:ex-type (type e)})))))

(comment
  (def i-map (instance/start (io/file (io/resource "seed.txt"))
                             (io/file "store-dir")
                             {:max-urls 1000
                              ;; :nb-fetchers 1 :nb-parsers 1
                              :extra-info true
                              :http-get aleph-http-get}))

  (do (instance/stop i-map) nil)
  )


(nippy/extend-freeze aleph.http.core.NettyResponse :netty-response/serialize
                     [this os]
                     (byte-serializer/write-array os (-> this
                                                         (select-keys  [:url :status :body])
                                                         nippy/freeze)))

(nippy/extend-thaw :netty-repsonse/serialize
                   [is]
                   (-> is byte-serializer/read-array nippy/thaw))
