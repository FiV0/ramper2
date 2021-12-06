(ns ramper.workbench.simple-bench
  (:require [clojure.data.priority-map :as pm]
            [ramper.url :as url]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.robots-txt :as robots-txt]))

;; TODO improve key for memory
;; TODO add cleanup loop for empty

(defn entry [url {:keys [robots-txt] :as _opts}]
  {:next-fetch (System/currentTimeMillis)
   :queue (cond-> clojure.lang.PersistentQueue/EMPTY
            robots-txt (conj (str (robots-txt/robots-txt url)))
            true (conj url))})

(defn add-url
  [entry url]
  (update entry :queue conj url))

(defn entry-empty? [entry]
  (nil? (seq (:queue entry))))

(defn entry-size [entry] (count (:queue entry)))

(defn simple-bench
  ([] (simple-bench {}))
  ([{:keys [robots-txt]}]
   (cond-> {:size 0
            :delay-queue (pm/priority-map-keyfn :next-fetch)
            :blocked {}
            :empty {}}
     robots-txt (assoc :robots-txt true))))

(defn cons-bench [{:keys [delay-queue blocked empty] :as bench} url]
  (let [base (url/base url)
        bench (update bench :size inc)]
    (cond-let
      (contains? delay-queue base)
      (->> (update delay-queue base add-url url)
           (assoc bench :delay-queue))

      (contains? blocked base)
      (->> (update blocked base add-url url)
           (assoc bench :blocked))

      [entry (get empty base)]
      (-> bench
          (update :empty dissoc base)
          (update :delay-queue assoc base (add-url entry url)))

      :els
      ;; TODO think about how to handle robots.txt when entries get purged
      (update bench :delay-queue assoc base (entry url bench)))))

(defn peek-bench [{:keys [delay-queue] :as _bench}]
  (let [[_ {:keys [queue next-fetch] :as entry}] (peek delay-queue)]
    (when (and entry (<= next-fetch (System/currentTimeMillis)))
      (peek queue))))

(defn pop-bench [{:keys [delay-queue] :as bench}]
  (let [[base {:keys [queue next-fetch] :as entry}] (peek delay-queue)]
    (if (and entry (<= next-fetch (System/currentTimeMillis)))
      (let [bench (update bench :size dec)]
        (cond-> (update bench :blocked assoc base (assoc entry :queue (pop queue)))
          (seq delay-queue) (update :delay-queue pop)))
      bench)))

(comment
  (def bench (atom (simple-bench)))

  (swap! bench cons-bench "https://finnvolkel.com")
  (swap! bench cons-bench "https://hckrnews.com")
  (peek-bench @bench)
  (-> @bench pop-bench peek-bench)
  (-> @bench pop-bench peek-bench peek-bench)
  )

(defn purge [{:keys [delay-queue blocked empty] :as bench} url]
  (let [base (url/base url)]
    (cond-let
      [entry (get delay-queue base)]
      (-> bench
          (update :size - (entry-size entry))
          (update :delay-queue dissoc base))

      [entry (get blocked base)]
      (-> bench
          (update :size - (entry-size entry))
          (update bench :blocked dissoc base))

      (contains? empty base)
      (update bench :empty dissoc base)

      :else bench)))

(comment
  (-> @bench (purge "https://finnvolkel.com/about") peek-bench)
  (-> @bench (purge "https://finnvolkel.com/about") peek-bench peek-bench)
  )

;; TODO this could potentially also be sped up by not doing the calculations
;; in peek and pop twice
(defn dequeue! [bench-atom]
  (loop []
    (let [old-bench     @bench-atom
          value (peek-bench old-bench)
          new-bench (pop-bench old-bench)]
      (cond (nil? value) nil
            (compare-and-set! bench-atom old-bench new-bench) value
            :else (recur)))))

(comment
  (dequeue! bench)
  @bench

  )

(defn readd [{:keys [blocked] :as bench} url next-fetch]
  {:pre [(contains? blocked (url/base url))]}
  (let [base (url/base url)
        entry (-> (get blocked base) (assoc :next-fetch next-fetch))
        bench (update bench :blocked dissoc base)]
    (if (entry-empty? entry)
      (update bench :empty assoc base entry)
      (update bench :delay-queue assoc base entry))))

(defn size [{:keys [size] :as _bench}] size)

(defn available-size [{:keys [delay-queue] :as _bench}] (count delay-queue))

(comment
  (swap! bench cons-bench "https://finnvolkel.com/about")
  (swap! bench cons-bench "https://hckrnews.com/foo")
  (swap! bench readd "https://finnvolkel.com/" (+ (System/currentTimeMillis) 1000))
  (swap! bench readd "https://hckrnews.com/" (+ (System/currentTimeMillis) 10000))

  (peek-bench @bench)
  (-> @bench pop-bench peek-bench)
  (-> @bench pop-bench pop-bench peek-bench)

  (-> @bench :empty count)
  (-> @bench :delay-queue count)
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
