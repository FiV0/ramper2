(ns ramper.workbench.simple-bench.wrapped
  "A higher level way to use ramper.workbench.simple-bench that assumes the
  immutable bench is wrapped in an atom.

  Inspired by clojure.core.cache.wrapped."
  (:require [ramper.workbench :refer [cons-bench! peek-bench pop-bench! purge! dequeue! readd!]]
            [ramper.workbench.simple-bench :as bench]))

(defn simple-bench-factory []
  (atom (bench/simple-bench)))

(defmethod cons-bench! :simple-bench [bench url]
  (swap! bench bench/cons-bench url))

(defmethod peek-bench :simple-bench [bench]
  (bench/peek-bench @bench))

(defmethod pop-bench! :simple-bench [bench]
  (swap! bench bench/pop-bench))

(defmethod purge! :simple-bench [bench url]
  (swap! bench bench/purge url))

(defmethod dequeue! :simple-bench [bench]
  (loop []
    (let [old-bench     @bench
          value (bench/peek-bench old-bench)
          new-bench (bench/pop-bench old-bench)]
      (cond (nil? value) nil
            (compare-and-set! bench old-bench new-bench) value
            :else (recur)))))

(defmethod readd! :simple-bench [bench url next-fetch]
  (swap! bench bench/readd url next-fetch))
