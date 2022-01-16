(ns repl-sessions.instance-testing
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [ramper.customization :as custom]
            [ramper.instance :as instance :refer [start stop pause]]
            [ramper.parser.html :as html]
            [ramper.util.threadpool :as threadpool]))

(def pool (threadpool/create-threadpool 4 8))

(defn contains-clojure? [resp]
  (some-> resp :body html/html->text str/lower-case (clojure.string/index-of "clojure")))

(def combis [{:sieve-type :memory :bench-type :memory}
             {:sieve-type :mercator :bench-type :memory}
             {:sieve-type :memory :bench-type :virtualized}
             {:sieve-type :mercator :bench-type :virtualized}])

(defn to-three-chars [keyword]
  (apply str (take 3 (drop 1 (str keyword)))))

(defn test-4-types [opts]
  (let [res (doall (for [{:keys [sieve-type bench-type] :as combi} combis]
                     (let [m (start (io/file (io/resource "seed.txt")) (io/file "store-dir") (merge opts combi))
                           t (async/<!! (:time-chan m))]
                       (str (to-three-chars sieve-type) "   "
                            (to-three-chars bench-type) "   "
                            (double (/ t 1000)) "sec"))))]
    (println "sieve bench time")
    (doseq [line res]
      (println line))))

(comment
  (test-4-types {:max-urls 10000
                 :robots-txt false
                 :http-opts {:proxy-url "http://localhost:8080"}
                 :extra-info true})

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 20000}))

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 100000
                                                                              :robots-txt true
                                                                              :http-opts {:proxy-url "http://localhost:8080"}
                                                                              :nb-fetchers 12 :nb-parsers 5
                                                                              :extra-info true
                                                                              ;; :new false
                                                                              ;; :store-filter contains-clojure?
                                                                              ;; :follow-filter contains-clojure?
                                                                              ;; :schedule-filter (custom/max-per-domain-filter 100)
                                                                              #_(every-pred custom/https-filter clojure-url?)
                                                                              ;; :sieve-type :mercator
                                                                              ;; :bench-type :virtualized
                                                                              }))

  ;; sieve bench time (with 100000 proxy urls) time (without timeout in emitter)
  ;; mem   mem  22sec                          25sec
  ;; mer   mem  30sec                          29sec
  ;; mem   vir  29sec                          25sec
  ;; mer   vir  29sec                          27sec

  (pause s-map)


  (def s-map (start (io/file (io/resource "seed.txt"))
                    (io/file "store-dir")
                    {:max-urls 10000 :nb-fetchers 2
                     :nb-parsers 1 :sieve-type :mercator}))

  (do (stop s-map) nil)
  (def s-map nil)

  (async/<!! (:time-chan s-map))

  )
