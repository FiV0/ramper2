(ns ramper.workbench.virtualized-bench.wrapped
  "A higher level way to use ramper.workbench.virtualized-bench."
  (:require [ramper.workbench :refer [cons-bench! peek-bench pop-bench! purge! dequeue! readd!]]
            [ramper.workbench.virtualized-bench :as bench]))

(defn virtualized-bench-factory []
  (atom (bench/virtualized-bench)))

(defmethod cons-bench! :virtualized-bench [bench url]
  (swap! bench bench/cons-bench url))

(defmethod peek-bench :virtualized-bench [bench]
  (bench/peek-bench @bench))

(defmethod pop-bench! :virtualized-bench [bench]
  (swap! bench bench/peek-bench))

(defmethod purge! :virtualized-bench [bench url]
  (swap! bench bench/purge bench url))

(defmethod dequeue! :virtualized-bench [bench]
  (loop []
    (let [old-bench @bench
          value (bench/peek-bench old-bench)
          new-bench (bench/pop-bench old-bench)]
      (cond (nil? value) nil
            (compare-and-set! bench old-bench new-bench) value
            :else (recur)))))

(defmethod readd! :virtualized-bench [bench url next-fetch]
  (swap! bench bench/readd url next-fetch))

(comment
  (def bench (virtualized-bench-factory))

  (binding [bench/max-per-key 2]
    (cons-bench! bench "https://finnvolkel.com")
    (cons-bench! bench "https://hckrnews.com")
    (cons-bench! bench "https://finnvolkel.com/about")
    (cons-bench! bench "https://finnvolkel.com/tech"))

  (binding [bench/max-per-key 2]
    (dequeue! bench))

  (binding [bench/max-per-key 2]
    (readd! bench *1 (- (System/currentTimeMillis) 100)))

  )
