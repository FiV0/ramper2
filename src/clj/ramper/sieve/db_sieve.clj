(ns ramper.sieve.db-sieve
  (:require [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [ramper.util :as util]))

(def con (atom (jdbc/get-connection
                (jdbc/get-datasource {:dbtype "sqlite"
                                      :dbname (io/file (util/temp-dir "db-sieve") "sieve.db")}))))

(def table-creation-stm " CREATE TABLE sieve ( key INT UNIQUE);")

(defn create-table [con]
  (jdbc/execute! con [table-creation-stm]))

(defn add-key [con key]
  (jdbc/execute! con [(str "INSERT INTO sieve(key) VALUES(" key ")")]))

(defn- interleave-keys [keys]
  (subs (apply str (interleave (repeat ",") (map #(str "(" % ")") keys) )) 1))

(comment
  (interleave-keys [1 2]))

(defn add-keys [con keys]
  (jdbc/execute! con [(str "INSERT INTO sieve(key) VALUES" (interleave-keys keys))]))


(defn get-everything [con]
  (jdbc/execute! con ["SELECT * FROM sieve;"]))

(defn get-key [con key]
  (jdbc/execute! con ["SELECT * FROM sieve WHERE key = ?" key]))

(comment
  (create-table @con)
  (add-key @con 123)
  (get-everything @con)
  (get-key @con 123)
  (add-keys @con [1 2 3]))

(defrecord DbSieve [con new hash-fn]
  java.io.Closeable
  (close [_this] (.close @con)))

(defn db-sieve []
  (reset! con (jdbc/get-connection
               (jdbc/get-datasource {:dbtype "sqlite"
                                     :dbname (io/file (util/temp-dir "db-sieve") "sieve.db")})))
  (create-table @con)
  (->DbSieve
   con
   clojure.lang.PersistentQueue/EMPTY
   (fn [s] (-> s hash long))))

(defn enqueue! [{:keys [hash-fn] :as sieve} key]
  (let [hash-key (hash-fn key)]
    (if-not (seq (get-key @con hash-key))
      (do
        (add-key @con hash-key)
        (update sieve :new conj key))
      sieve)))

(defn- in-sieve? [con hash-key]
  (seq (get-key con hash-key)))

(defn enqueue*! [{:keys [hash-fn] :as sieve} keys]
  (let [new-key-hash-pairs (->> (map #(vector % (hash-fn %)) keys)
                                (remove #(in-sieve? @con (second %))))]
    (if (seq new-key-hash-pairs)
      (do
        (add-keys @con (map second new-key-hash-pairs))
        (update sieve :new into (map first new-key-hash-pairs)))
      sieve)))

(defn peek-sieve [{:keys [new] :as _sieve}]
  (peek new))

(defn pop-sieve [sieve]
  (update sieve :new pop))

(comment
  (def sieve (atom (db-sieve)))

  (swap! sieve enqueue! "abc")
  (swap! sieve enqueue! "bcd")
  (swap! sieve enqueue*! ["abd" "bcd" "abc"])

  (peek-sieve @sieve)
  (swap! sieve pop-sieve)


  )
