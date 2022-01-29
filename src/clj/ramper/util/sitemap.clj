(ns ramper.util.sitemap
  (:require [clojure.data.xml :as xml]))

(defn url-entry->url-map [{:keys [content] :as _url-entry}]
  (let [[loc] (filter #(= :loc (:tag %)) content)
        [lastmod] (filter #(= :lastmod (:tag %)) content)
        [changefreq] (filter #(= :changefreq (:tag %)) content)]
    (cond-> {:url (first (:content loc))}
      lastmod (assoc :lastmod (first (:content lastmod)))
      changefreq (assoc :changefreq (first (:content changefreq))))))

(defn parse-sitemap [s]
  (some->> (xml/parse (java.io.StringReader. s))
           :content
           (map url-entry->url-map)))

(comment

  (def clj-sitemap (slurp "https://clojure.org/sitemap.xml"))

  (parse-sitemap clj-sitemap)


  )
