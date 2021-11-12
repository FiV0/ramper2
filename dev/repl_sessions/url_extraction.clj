(ns repl-sessions.url-extraction
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [ramper.util :as util]))

;; creating some seed urls from my bookmarks
(def bookmarks (-> (io/resource "bookmarks.json")
                   io/reader
                   (json/read :key-fn keyword)))

(defn extract-uris [obj]
  (cond
    (:uri obj) (list (:uri obj))
    (map? obj) (->> obj
                    (filter (fn [[_ v]] (seqable? v)))
                    (mapcat (fn [[_ v]] (mapcat extract-uris v))))
    :else '()))

(def urls (->> (extract-uris bookmarks)
               (filter #(str/starts-with? % "http"))
               distinct
               sort))

(util/write-urls (io/file "resources/seed.txt") urls)
