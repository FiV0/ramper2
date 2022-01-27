(ns ramper.parser.html
  (:require [clojure.java.io :as io]
            [lambdaisland.uri :as uri]
            [ramper.url :as url]
            [ramper.parser :as parser])
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
      BufferedInputStream.
      Source.))

;; TODO maybe use FastBufferedInputStream
(defmethod source ^Source InputStream [^InputStream html-stream]
  (let [res (-> html-stream
                BufferedInputStream.
                Source.)]
    (when (.markSupported html-stream)
      (.reset html-stream))
    res))

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


(defn parse [content-type content]
  (let [source (source content)]
    (.fullSequentialParse source)
    {:content-type content-type :parsed-content source}))

;; application/x-asp ?
;; application/vnd.wap.xhtml+xml ?
(defmethod parser/parse "text/html" [_ content]
  (parse "text/html" content))
(defmethod parser/parse "application/xhtml+xml" [_ content]
  (parse "application/xhtml+xml" content))

(defmethod parser/extract-text "text/html" [{:keys [parsed-content]}]
  (-> ^Source parsed-content TextExtractor. .toString))
(defmethod parser/extract-text "application/xhtml+xml" [{:keys [parsed-content]}]
  (-> ^Source parsed-content TextExtractor. .toString))

(defn source->links [^Source source]
  (let [tags (-> source (.getAllElements HTMLElementName/A))]
    (->> (remove (fn [^Element tag] (or (some-> tag (.getAttributeValue "rel") (.contains "nofollow"))
                                        (nil? (.getAttributeValue tag "href")))) tags)
         (map (fn [^Element tag] (.getAttributeValue tag "href"))))))

(defmethod parser/extract-links "text/html" [{:keys [parsed-content]}]
  (source->links parsed-content))

(defmethod parser/extract-links "application/xhtml+xml" [{:keys [parsed-content]}]
  (source->links parsed-content))

(comment
  (def base "https://hckrnews.com")
  (def links (-> "https://hckrnews.com" slurp html->links))

  (create-new-urls base links)

  (require '[org.httpkit.client :as http])

  (def res @(http/get "https://finnvolkel.com"
                      {:follow-redirects false :timeout 3000 :as :text }))

  (def parsed (parser/parse (-> res :headers :content-type) (:body res)))

  (parser/extract-links parsed)
  (parser/extract-text parsed)
  )
