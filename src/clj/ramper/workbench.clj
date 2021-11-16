(ns ramper.workbench)

(defprotocol Workbench
  (cons-bench! [this url])
  (peek-bench [this])
  (pop-bench! [this])
  (purge! [this url])
  (dequeue! [this])
  (readd! [this url next-fetch]))

(defmulti create-workbench (fn [type & _args] type))

(defmethod create-workbench :default [type & _args]
  (throw (IllegalArgumentException. (str "No such workbench: " type))))
