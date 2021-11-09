(ns ramper.bench)

(defprotocol Workbench
  (cons-bench! [this url])
  (peek-bench [this])
  (pop-bench! [this])
  (purge! [this url])
  (dequeue! [this])
  (readd! [this url next-fetch]))
