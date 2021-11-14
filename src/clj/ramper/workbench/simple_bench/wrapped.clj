(ns ramper.workbench.simple-bench.wrapped
  "A higher level way to use ramper.workbench.simple-bench that assumes the
  immutable bench is wrapped in an atom.

  Inspired by clojure.core.cache.wrapped."
  (:require [ramper.workbench :refer [Workbench]]
            [ramper.workbench.simple-bench :as bench]))

(defn cons-bench [bench url]
  (swap! bench bench/cons-bench url))

(defn peek-bench [bench]
  (bench/peek-bench @bench))

(defn pop-bench [bench]
  (swap! bench bench/pop-bench))

(defn purge [bench url]
  (swap! bench bench/purge url))

(defn dequeue!
  [bench]
  (loop []
    (let [old-bench     @bench
          value (bench/peek-bench old-bench)
          new-bench (bench/pop-bench old-bench)]
      (cond (nil? value) nil
            (compare-and-set! bench old-bench new-bench) value
            :else (recur)))))

(defn readd [bench url next-fetch]
  (swap! bench bench/readd url next-fetch))

(defrecord SimpleBench [bench-atom]
  Workbench
  (cons-bench! [_this url] (cons-bench bench-atom url))
  (peek-bench [_this] (peek-bench bench-atom))
  (pop-bench! [_this] (pop-bench bench-atom))
  (purge! [_this url] (purge bench-atom url))
  (dequeue! [_this] (dequeue! bench-atom))
  (readd! [_this url next-fetch] (readd bench-atom url next-fetch)))

(defn simple-bench-factory []
  (->SimpleBench (atom (bench/simple-bench))))
