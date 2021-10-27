(ns ramper.bench
  (:require [ramper.url :as url]))

(def bench (atom clojure.lang.PersistentQueue/EMPTY))

(defn init []
  (reset! bench {}))

(defn add [bench url]
  (conj bench url))

(defn get-url [bench]
  (peek bench))

(defn dequeue! [bench-atom] (ffirst (swap-vals! bench-atom pop)))

(comment
  (swap! bench add "https://foobar.com")
  (get-url @bench)
  (dequeue! bench)

  )
