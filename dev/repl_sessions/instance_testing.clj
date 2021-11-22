(ns repl-sessions.instance-testing
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [ramper.customization :as custom]
            [ramper.html-parser :as html]
            [ramper.instance :as instance :refer [start stop]]
            [ramper.util.threadpool :as threadpool]))

(def pool (threadpool/create-threadpool 4 8))

(defn contains-clojure? [resp]
  (some-> resp :body html/html->text str/lower-case (clojure.string/index-of "clojure")))

(comment
  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 20000}))

  (def s-map (start (io/file (io/resource "seed.txt")) (io/file "store-dir") {:max-urls 10000
                                                                              :http-opts {:proxy-url "http://localhost:8080"}
                                                                              ;;:nb-fetchers 1 :nb-parsers 1
                                                                              :extra-info true
                                                                              ;; :store-filter contains-clojure?
                                                                              ;; :follow-filter contains-clojure?
                                                                              ;; :schedule-filter (custom/max-per-domain-filter 100)
                                                                              #_(every-pred custom/https-filter clojure-url?)
                                                                              ;; :sieve-type :mercator
                                                                              #_#_:bench-type :virtualized}))
  ;; sieve bench time (with 100000 proxy urls) time (without timeout in emitter)
  ;; mem   mem  22sec                          25sec
  ;; mer   mem  30sec                          29sec
  ;; mem   vir  29sec                          25sec
  ;; mer   vir  29sec                          27sec


  (def s-map (start (io/file (io/resource "seed.txt"))
                    (io/file "store-dir")
                    {:max-urls 10000 :nb-fetchers 2
                     :nb-parsers 1 :sieve-type :mercator}))

  (do (stop s-map) nil)
  (def s-map nil)

  (async/<!! (:time-chan s-map))

  )
