(ns ramper.customization
  "A namespace with some generic filters for the customization of an instance."
  (:require [lambdaisland.uri :as uri]
            [ramper.url :as url]))

;; TODO make more efficient
(defn max-per-domain-filter
  "Returns a function that accepts at most `max-per-domain` urls per domain."
  [max-per-domain]
  (let [domain-to-count (atom {})]
    (fn [url]
      (let [base (url/base url)]
        (when (< (get @domain-to-count base 0) max-per-domain)
          (swap! domain-to-count update base (fnil inc 0))
          true)))))

(defn http? [url]
  (= "http" (:scheme (uri/uri url))))

(defn https? [url]
  (= "https" (:scheme (uri/uri url))))

(comment
  (def max-filter (max-per-domain-filter 2))

  (max-filter "http://finnvolkel.com/about")

  )
