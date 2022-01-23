(ns ramper.parser.html
  (:require [clojure.java.io :as io]
            [lambdaisland.uri :as uri]
            [ramper.url :as url])
  (:import (java.io InputStream BufferedInputStream)
           (net.htmlparser.jericho Attribute Element HTMLElementName
                                   Source StartTagType TextExtractor)))

(defmulti source (fn [html] (type html)))
(defmethod source :default [html]
  (throw (Exception. "No source method of html of type: " (type html))))

(defmethod source ^Source String [^String html-str]
  (-> html-str
      .getBytes
      io/input-stream
      java.io.BufferedInputStream.
      Source.))

;; TODO maybe use FastBufferedInputStream
(defmethod source ^Source InputStream [^InputStream html-stream]
  (-> html-stream
      (BufferedInputStream.)
      Source.))

(defn html->text [html-str]
  (-> html-str source TextExtractor. .toString))

(defn html->links  [html-str]
  (let [tags (-> html-str source (.getAllElements HTMLElementName/A))]
    (->> (remove (fn [^Element tag] (or (some-> tag (.getAttributeValue "rel") (.contains "nofollow"))
                                        (nil? (.getAttributeValue tag "href")))) tags)
         (map (fn [^Element tag] (.getAttributeValue tag "href"))))))

(defn create-new-urls [base-url links]
  (->> links
       (map #(uri/join base-url (uri/uri %)))
       (map url/normalize)
       distinct))

(comment
  (def base "https://hckrnews.com")
  (def links (-> "https://hckrnews.com" slurp html->links))

  (create-new-urls base links)

  (require '[org.httpkit.client :as http])

  (def res @(http/get "https://finnvolkel.com"
                      {:follow-redirects false :timeout 3000 :as :stream}))

  (-> res :body source)
  )
