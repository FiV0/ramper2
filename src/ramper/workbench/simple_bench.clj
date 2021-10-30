(ns ramper.workbench.simple-bench
  (:require [clojure.data.priority-map :as pm]
            [ramper.url :as url]))

;; TODO improve key for memory

(defn entry [url]
  {:next-fetch (System/currentTimeMillis)
   :queue (conj clojure.lang.PersistentQueue/EMPTY url)})

(defn add-url
  [entry url]
  (update entry :queue conj url))

(defn simple-bench [] {:delay-queue (pm/priority-map-keyfn :next-fetch) :blocked {}})

(defn cons-bench [{:keys [delay-queue blocked] :as bench} url]
  (let [base (url/base url)]
    (cond
      (contains? delay-queue base) (->> (update delay-queue base update :queue conj url)
                                        (assoc bench :delay-queue))
      (contains? blocked base) (->> (update blocked base update :queue conj url)
                                    (assoc bench :blocked))
      :else (update bench :delay-queue assoc base (entry url)))))

(defn peek-bench [{:keys [delay-queue] :as _bench}]
  (let [[_ {:keys [queue] :as _entry}] (peek delay-queue)]
    (peek queue)))

(defn pop-bench [{:keys [delay-queue] :as bench}]
  (let [[base {:keys [queue] :as entry}] (peek delay-queue)]
    (cond-> (update bench :blocked assoc base (assoc entry :queue (pop queue)))
      (seq delay-queue) (update :delay-queue pop))))

(comment
  (def bench (atom (simple-bench)))

  (swap! bench cons-bench "https://finnvolkel.com")
  (swap! bench cons-bench "https://hckrnews.com")
  (peek-bench @bench)
  (-> @bench pop-bench peek-bench)
  (-> @bench pop-bench peek-bench peek-bench)
  )

(defn purge [{:keys [delay-queue blocked] :as bench} url]
  (let [base (url/base url)]
    (cond (contains? delay-queue base) (update bench :delay-queue dissoc base)
          (contains? blocked base) (update bench :blocked dissoc base)
          :else bench)))

(comment
  (-> @bench (purge "https://finnvolkel.com/about") peek-bench)
  (-> @bench (purge "https://finnvolkel.com/about") peek-bench peek-bench)
  )

(defn dequeue!
  [bench-atom]
  (loop []
    (let [old-bench     @bench-atom
          value (peek-bench old-bench)
          new-bench (pop-bench old-bench)]
      (cond (nil? value) nil
            (compare-and-set! bench-atom old-bench new-bench) value
            :else (recur)))))

(comment
  (dequeue! bench)

  )

(defn readd [{:keys [blocked] :as bench} url next-fetch]
  {:pre [(contains? blocked (url/base url))]}
  (let [base (url/base url)
        entry (-> (get blocked base) (assoc :next-fetch next-fetch))]
    (-> bench
        (update :blocked dissoc base)
        (update :delay-queue assoc base entry))))

(comment
  (swap! bench cons-bench "https://finnvolkel.com/about")
  (swap! bench cons-bench "https://hckrnews.com/foo")
  (swap! bench readd "https://finnvolkel.com/" (+ (System/currentTimeMillis) 1000))
  (swap! bench readd "https://hckrnews.com/" (+ (System/currentTimeMillis) 500))

  (peek-bench @bench)
  (-> @bench pop-bench peek-bench)
  (-> @bench pop-bench pop-bench peek-bench)
  )

(comment
  (require '[clojure.data.priority-map :as pm])

  (def test-pm (pm/priority-map-keyfn first "foo" [1 {:name "foo"}] "bar" [2 {:name "bar"}]))

  (peek test-pm)
  (peek (pm/priority-map-keyfn first))
  (update test-pm "foo" (fn [[p m]] [p (assoc m :hello :world)]))
  (contains? test-pm "foo")
  (contains? test-pm "toto")
  (pop test-pm)
  (-> test-pm pop pop pop)

  (def huge-pm (into (pm/priority-map) (map vector (range 1000000) (range 1000000))))
  (time (empty? huge-pm))
  (time (seq huge-pm))

  )
