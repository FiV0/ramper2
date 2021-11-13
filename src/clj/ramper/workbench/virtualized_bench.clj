(ns ramper.workbench.virtualized-bench
  "IMPORTANT: not thread-safe"
  (:require [clojure.data.priority-map :as pm]
            [ramper.url :as url]
            [ramper.util :as util]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.data-disk-queues :as ddq]))

(defn entry [url]
  {:base (str (url/base url))
   :next-fetch (System/currentTimeMillis)
   :queue (conj clojure.lang.PersistentQueue/EMPTY url)})

(defn add-url
  [entry url]
  (update entry :queue conj url))

(defn add-urls
  [entry urls]
  (update entry :queue into urls))

(defn entry-empty? [entry]
  (boolean (seq (:queue entry))))

(defn entry-size [entry] (count (:queue entry)))

(defn entry-key [{:keys [base]}] (hash base))

(def ^:dynamic max-per-key 100)

(defn virtualized-bench []
  {:delay-queue (pm/priority-map-keyfn :next-fetch)
   :blocked {}
   :empty {}
   :ddq (ddq/data-disk-queues (util/temp-dir "virtualized-bench"))})

(defn cons-bench [{:keys [delay-queue blocked empty ddq] :as bench} url]
  (let [base (url/base url)]
    (cond-let
     [entry (get delay-queue base)]
     (if (<= (entry-size entry) max-per-key)
       (->> (update delay-queue base add-url url)
            (assoc bench :delay-queue))
       (do
         (ddq/enqueue ddq (entry-key entry) url)
         bench))

     [entry (get blocked base)]
     (if (<= (entry-size entry) max-per-key)
       (->> (update blocked base add-url url)
            (assoc bench :blocked))
       (do
         (ddq/enqueue ddq (entry-key entry) url)
         bench))

     [entry (get empty base)]
     (if (<= (entry-size entry) max-per-key)
       (-> bench
           (update :empty dissoc base)
           (update :delay-queue assoc base (add-url entry url)))
       (do
         (ddq/enqueue ddq (entry-key entry) url)
         bench))

     :else (update bench :delay-queue assoc base (entry url)))))

(defn peek-bench [{:keys [delay-queue] :as _bench}]
  (let [[_ {:keys [queue next-fetch] :as entry}] (peek delay-queue)]
    (when (and entry (<= next-fetch (System/currentTimeMillis)))
      (peek queue))))

(defn pop-bench [{:keys [delay-queue] :as bench}]
  (let [[base {:keys [queue next-fetch] :as entry}] (peek delay-queue)]
    (if (and entry (<= next-fetch (System/currentTimeMillis)))
      (cond-> (update bench :blocked assoc base (assoc entry :queue (pop queue)))
        (seq delay-queue) (update :delay-queue pop))
      bench)))

(comment
  (binding [max-per-key 2]
    (def bench (-> (virtualized-bench)
                   (cons-bench "https://finnvolkel.com")
                   (cons-bench "https://hckrnews.com")
                   (cons-bench "https://finnvolkel.com/about")
                   (cons-bench "https://finnvolkel.com/foofoo"))))

  (binding [max-per-key 2]
    (println (peek-bench bench))
    (println (-> bench pop-bench peek-bench))
    (println (-> bench pop-bench pop-bench peek-bench)))
  )

(defn purge [{:keys [delay-queue blocked empty ddq] :as bench} url]
  (let [base (url/base url)]
    ;; TODO fix hash to use entry
    (ddq/remove ddq (hash base))
    (cond (contains? delay-queue base) (update bench :delay-queue dissoc base)
          (contains? blocked base) (update bench :blocked dissoc base)
          (contains? empty base) (update bench :empty dissoc base)
          :else bench)))

(comment
  (-> bench (purge "https://finnvolkel.com/about") peek-bench)
  (-> bench (purge "https://finnvolkel.com/about") pop-bench peek-bench)
  )

(defn- refill-entry [entry ddq]
  (if (<= (entry-size entry) max-per-key)
    (let [key (entry-key entry)]
      (->> (repeatedly (min (ddq/count ddq key) (- max-per-key (entry-size entry))) #(ddq/dequeue ddq key))
           (add-urls entry)))
    entry))

(defn readd [{:keys [blocked ddq] :as bench} url next-fetch]
  {:pre [(contains? blocked (url/base url))]}
  (let [base (url/base url)
        entry (-> (get blocked base)
                  (assoc :next-fetch next-fetch)
                  (refill-entry ddq))
        bench (update bench :blocked dissoc base)]
    (if (entry-empty? entry)
      (update bench :delay-queue assoc base entry)
      (update bench :empty assoc base entry))))

(comment
  (binding [max-per-key 2]
    (let [url (peek-bench bench)
          bench (nth (iterate pop-bench bench) 2)
          bench (readd bench url (- (System/currentTimeMillis) 100))
          url2 (peek-bench bench)
          bench (pop-bench bench)
          bench (readd bench url2 (- (System/currentTimeMillis) 100)) ]
      (println (peek-bench bench))))

  )
