(ns ramper.sieve.memory-sieve
  (:require [ramper.sieve :refer [Sieve create-sieve]]))

(defn sieve [] {:seen #{} :new clojure.lang.PersistentQueue/EMPTY})

(defn add [{:keys [seen] :as sieve} url]
  (cond-> (update sieve :seen conj url)
    (not (seen url)) (update :new conj url)))

(defn add* [{:keys [seen] :as sieve} urls]
  (let [not-seen (remove seen urls)]
    (-> sieve
        (update :seen into not-seen)
        (update :new into not-seen))))

(defn peek-sieve [{:keys [new] :as _sieve}]
  (peek new))

(defn pop-sieve [sieve]
  (update sieve :new pop))

;; To make it consistent with possible other implementations

(defn dequeue! [sieve-atom]
  (peek-sieve (first (swap-vals! sieve-atom pop-sieve))))

(defrecord MemorySieve [sieve-atom]
  Sieve
  (enqueue! [_this key]
    (swap! sieve-atom add key))
  (enqueue*! [_this keys]
    (swap! sieve-atom add* keys))
  (dequeue! [_this]
    (peek-sieve (first (swap-vals! sieve-atom pop-sieve)))))

(defn memory-sieve [] (->MemorySieve (atom (sieve))))

(defmethod create-sieve :memory [_ & _args]
  (memory-sieve))

(comment
  (def the-sieve (atom (sieve)))
  (swap! the-sieve add "https://foobar.com")
  (swap! the-sieve add "https://foobar.com")
  (swap! the-sieve add "https://foobar2.com")
  (-> @the-sieve peek-sieve)
  (-> @the-sieve pop-sieve peek-sieve)
  (-> @the-sieve pop-sieve pop-sieve peek-sieve)

  (dequeue! the-sieve)

  (swap! the-sieve add* ["a" "https://foobar2.com" "b" "https://foobar.com"]))
