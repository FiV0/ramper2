(ns ramper.parser.text
  (:require [ramper.parser :as parser])
  (:import [java.io InputStream]))

(defmethod parser/parse "text/plain" [content-type content]
  {:content-type content-type :parsed-content content})

(defmethod parser/extract-text "text/plain" [{:keys [parsed-content]}]
  (cond-> parsed-content (instance? InputStream parsed-content) slurp))

(defmethod parser/extract-links "text/plain" [{:keys [parsed-content]}]
  ;; TODO
  nil)

(comment
  (require '[org.httpkit.client :as client]
           '[clojure.java.io :as io])

  (def clojure-robots-txt @(client/get "https://clojure.org/robots.txt" {:as :stream}))

  (-> (parser/parse (-> clojure-robots-txt :headers :content-type) (:body clojure-robots-txt))
      parser/extract-text)

  (-> (parser/parse (-> clojure-robots-txt :headers :content-type) (:body clojure-robots-txt))
      parser/extract-links)

  )
