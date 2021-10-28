(ns ramper.url
  (:refer-clojure :exclude [uri?])
  (:require [lambdaisland.uri :as uri])
  (:import (lambdaisland.uri URI)))

(defn base
  "Returns only the scheme + authority of an uri-like object as
  an URI."
  [uri-like]
  (let [{:keys [scheme host port]} (uri/uri uri-like)]
    (assoc (uri/uri "") :scheme scheme :host host :port port)))

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
