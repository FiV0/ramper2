(ns ramper.parser.xml
  (:require [clojure.data.xml :as xml]
            [ramper.parser :as parser]))

(defmethod parser/parse "application/xml" [_ content]
  {:content-type "application/xml"
   :parsed-content (xml/parse (cond->> content (string? content) (java.io.StringReader.)))})

(defmethod parser/extract-links "application/xml" [{:keys [parsed-content]}]
  nil)

(defn- leaf-seq
  [branch? children root]
  (let [walk (fn walk [node]
               (lazy-seq
                (if (branch? node)
                  (mapcat walk (children node))
                  (list node))))]
    (walk root)))

(defn- xml-leafs [root]
  (leaf-seq
   (complement string?)
   (comp seq :content)
   root))

(defmethod parser/extract-text "application/xml" [{:keys [parsed-content]}]
  (->> parsed-content
       xml-leafs
       (interpose " ")
       (apply str)))

(comment
  (require '[org.httpkit.client :as client]
           '[clojure.java.io :as io])

  (def res @(client/get "http://clojure.org/sitemap.xml" {:as :text}))
  (def res @(client/get "http://clojure.org/sitemap.xml" {:as :stream}))

  (def parsed (parser/parse (-> res :headers :content-type) (:body res)))

  (parser/extract-text parsed)

  (def doc (-> (io/resource "logback.xml") slurp))
  (def parsed2 {:content-type "application/xml" :parsed-content (xml/parse (java.io.StringReader. doc))})
  (parser/extract-text parsed2)
  )
