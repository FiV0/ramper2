(ns ramper.html-parser
  (:require [clojure.java.io :as io]
            [lambdaisland.uri :as uri]
            [ramper.url :as url])
  (:import (net.htmlparser.jericho Attribute Element HTMLElementName
                                   Source StartTagType TextExtractor)))

(defn source ^Source [^String html-str]
  (-> html-str
      .getBytes
      io/input-stream
      java.io.BufferedInputStream.
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

  )
