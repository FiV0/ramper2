(ns ramper.util.bloom-filter
  (:import (it.unimi.dsi.util BloomFilter)))

(defrecord BloomFilterWrapper [bloom-filter hash-function])

(defn bloom-filter
  ([hash-function] (bloom-filter 1000000 hash-function))
  ([n hash-function]
   (->BloomFilterWrapper (BloomFilter/create n 1E-8) hash-function)))

(defn bloom-filter-long
  ([hash-function] (bloom-filter-long 1000000 hash-function))
  ([n hash-function]
   (->BloomFilterWrapper (BloomFilter/create n BloomFilter/LONG_FUNNEL) hash-function)))

(defn add [bloom-filter item]
  (.add (:bloom-filter bloom-filter) ((:hash-function bloom-filter) item)))

(defn contains [bloom-filter item]
  (.contains (:bloom-filter bloom-filter) ((:hash-function bloom-filter) item)))

(defn contains-hash [bloom-filter item-hash]
  (.contains (:bloom-filter bloom-filter) item-hash))

(comment
  (require '[taoensso.nippy :as nippy])

  (def bf1 (bloom-filter (fn [item] (nippy/freeze item))))

  (add bf1 {:foo 1})
  (contains bf1 {:foo 1})
  (contains bf1 {:foo 2})

  (def bf2 (bloom-filter-long (fn [item] (-> item hash long))))

  (add bf2 {:foo 1})
  (contains bf1 {:foo 1})
  (contains bf1 {:foo 2})

  )
