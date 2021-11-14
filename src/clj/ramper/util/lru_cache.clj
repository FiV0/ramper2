(ns ramper.util.lru-cache
  "A thin wrapper around `clojure.core.cached`"
  (:require [clojure.core.cache.wrapped :as cw]))

(defprotocol Cache
  (add [this item] "Add an item to the cache")
  (check [this item] "Check whether item is in cache."))

(deftype LruCache [cache hash-fn]
  Cache
  (add [_this item]
    (cw/through-cache cache (hash-fn item) (constantly true)))
  (check [_this item]
    (cw/lookup cache (hash-fn item)))

  clojure.lang.Counted
  (count [_this]
    (count @cache)))

(defn create-lru-cache
  ([threshold hash-fn] (create-lru-cache {} threshold hash-fn))
  ([data threshold hash-fn]
   (->LruCache
    (cw/lru-cache-factory (into {} (map #(vector (hash-fn %) true) data)) :threshold threshold)
    hash-fn)))

(comment
  (def cache (create-lru-cache {} 2 (fn [x] (-> x hash long))))
  (add cache "foo bar")
  (check cache "foo bar")
  (check cache "dafafa")
  (add cache "dafafa")
  (add cache "dafafa1")
  (count cache))
