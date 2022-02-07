(ns ramper.url
  (:refer-clojure :exclude [uri?])
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri])
  (:import (lambdaisland.uri URI)))

(defn base
  "Returns only the scheme + authority of an `uri-like` object as
  an URI."
  [uri-like]
  (let [{:keys [scheme host port]} (uri/uri uri-like)]
    (assoc (uri/uri "") :scheme scheme :host host :port port)))

(defn path+queries
  "Returns only the path + queries of an `uri-like` object as an URI."
  [uri-like]
  (let [{:keys [path query]} (uri/uri uri-like)]
    (assoc (uri/uri "") :path path :query query)))

(defn normalize
  "Normalizes an `uri-like` object."
  [uri-like]
  (-> (uri/uri uri-like)
      (assoc :fragment nil
             :user nil
             :password nil)))

(defn uri?
  "Returns true if `uri-like` is a lambdaisland.uri.URI."
  [uri-like]
  (instance? URI uri-like))

(defn https->http
  "Swap out any https scheme with a http scheme"
  [uri-like]
  (-> (uri/uri uri-like)
      (assoc :scheme "http")))

(defn robots-txt? [url] (str/ends-with? url "robots.txt"))
(defn sitemap? [url] (str/ends-with? url "sitemap.xml"))
