(ns ramper.util.lru-cache
  "A thin wrapper around `clojure.core.cached`"
  (:require [clojure.core.cache.wrapped :as cw]))

(defprotocol Cache
  (check [this item]
    "Returns whether the cache contains the item or not. Updates the internal logic."))

(deftype LruCache [cache hash-fn]
  Cache
  (check [_this item]
    (let [hash (hash-fn item)
          res (cw/has? cache hash)]
      (cw/lookup-or-miss cache hash (constantly true))
      res))

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
  (check cache "foo bar")
  (check cache "dafafa")
  (check cache "dafafa1")
  (count cache))
