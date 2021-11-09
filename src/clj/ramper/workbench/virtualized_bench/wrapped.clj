(ns ramper.workbench.virtualized-bench.wrapped
  "A higher level way to use ramper.workbench.virtualized-bench."
  (:require [ramper.bench :refer [Workbench]]
            [ramper.workbench.virtualized-bench :as bench]))

(deftype VirtualizedBench [^:volatile-mutable bench]
  Workbench
  (cons-bench! [this url]
    (locking this
      (set! bench (bench/cons-bench bench url))))

  (peek-bench [_this]
    (bench/peek-bench bench))

  (pop-bench! [this]
    (locking this
      (set! bench (bench/pop-bench bench))))

  (purge! [this url]
    (locking this
      (set! bench (bench/purge bench url))))

  (dequeue! [this]
    (locking this
      (let [res (bench/peek-bench bench)]
        (set! bench (bench/pop-bench bench))
        res)))

  (readd! [this url next-fetch]
    (locking this
      (set! bench (bench/readd bench url next-fetch))))

  java.io.Closeable
  (close [_this]
    (bench/close bench)))

(defn virtualized-bench-factory []
  (->VirtualizedBench (bench/virtualized-bench)))

(comment
  (require '[ramper.bench :refer [cons-bench! dequeue! readd!]])
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
