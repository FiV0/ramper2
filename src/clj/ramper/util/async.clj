(ns ramper.util.async
  (:require [clojure.core.async :as async]))

(defn multi->!! [channel-val-pairs]
  (loop [cvs channel-val-pairs]
    (when (seq cvs)
      (let [[_ c] (async/alts!! cvs)]
        (recur (filter #(not= c (first %)) cvs))))))

;; version by @hiredman
(defn multi [channel-val-pairs]
  (let [c (async/chan (count channel-val-pairs))]
    (doseq [[c v] channel-val-pairs]
      (async/put! c v (fn [_] (async/put! c true))))
    (async/go
      (dotimes [_ (count channel-val-pairs)]
        (async/<! c)))))

(comment
  (def c1 (async/chan))
  (def c2 (async/chan))

  (async/go
    (let [[v1] (async/alts! [c1 c2])
          [v2] (async/alts! [c1 c2])]
      (println v1 v2)))

  (multi->!! [[c1 1] [c2 2]]))
