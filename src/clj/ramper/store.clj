(ns ramper.store
  "A set of protocols for a Store."
  (:refer-clojure :exclude [read]))

;; TODO: find a better solution
(def store-name "ramper_store")

(defprotocol Store
  (store
    [this data]
    [this data is-duplicate content-digest guessed-charset]
    "Stores some data for which a serializer exists in the store."))

(defprotocol StoreReader
  (read [this]
    "Reads a SimpleRecord from the store. Returns nil if no record is available."))

(defmulti create-store (fn [type & _args] type))

(defmethod create-store :default [type & _args]
  (throw (IllegalArgumentException. (str "No such store: " type))))
