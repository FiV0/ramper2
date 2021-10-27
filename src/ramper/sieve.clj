(ns ramper.sieve)

(defn sieve [] {:seen #{} :new clojure.lang.PersistentQueue/EMPTY})

(defn add [{:keys [seen] :as sieve} url]
  (cond-> (update sieve :seen conj url)
    (not (seen url)) (update :new conj url)))

(defn peek-sieve [{:keys [new] :as sieve}]
  (peek new))

(defn pop-sieve [sieve]
  (update sieve :new pop))

(defn dequeue! [sieve-atom]
  (peek-sieve (first (swap-vals! sieve-atom pop-sieve))))

(comment
  (def the-sieve (atom (sieve)))
  (swap! the-sieve add "https://foobar.com")
  (swap! the-sieve add "https://foobar.com")
  (swap! the-sieve add "https://foobar2.com")
  (-> @the-sieve peek-sieve)
  (-> @the-sieve pop-sieve peek-sieve)
  (-> @the-sieve pop-sieve pop-sieve peek-sieve)

  (dequeue! the-sieve)

  )
