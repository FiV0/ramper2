(ns ramper.workbench)

(defn- bench-type [bench]
  (cond (contains? @bench :dqq)
        :virtualized-bench
        (= clojure.lang.Atom (type bench))
        :simple-bench))

(defmulti cons-bench! (fn [bench _url] (bench-type bench)))

(defmulti peek-bench (fn [bench] (bench-type bench)))

(defmulti pop-bench! (fn [bench] (bench-type bench)))

(defmulti purge! (fn [bench _url] (bench-type bench)))

(defmulti dequeue! (fn [bench] (bench-type bench)))

(defmulti readd! (fn [bench _url _next-fetch] (bench-type bench)))

;; default methods

(defmethod cons-bench! :default [bench _url]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))

(defmethod peek-bench :default [bench]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))

(defmethod pop-bench! :default [bench]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))

(defmethod purge! :default [bench _url]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))

(defmethod dequeue! :default [bench]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))

(defmethod readd! :default [bench _url _next-fetch]
  (throw (IllegalArgumentException. (str "No such workbench: " (type bench)))))
