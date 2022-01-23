(ns ramper.store.simple-record
  "A simple record for storing a http response."
  (:refer-clojure :exclude [uri?])
  (:require [clojure.spec.alpha :as s]
            [lambdaisland.uri :as uri]
            [ramper.util.byte-serializer :as byte-serializer]
            [ramper.url :as url]
            [ramper.util :as util]
            [taoensso.nippy :as nippy])
  (:import (java.io IOException)
           (lambdaisland.uri URI)))

;; TODO: maybe choose a different name than record. It's already overloaded in Clojure.
;; TODO: adapt this so keys are modifiable by config
(defrecord SimpleRecord [url response])

;; TODO unify with fetched-data
(s/def :store/record (s/keys :req-un [::headers ::status ::body]))

(defn simple-record
  "Creates a simple record from an `url` and a `response`."
  [^URI url response]
  {:pre [(url/uri? url) (s/valid? :store/record response)]}
  (->SimpleRecord url response))

(s/fdef simple-record
  :args (s/cat :url url/uri? :response :store/record))

(defn prepare-response [resp]
  (-> resp
      (select-keys  [:headers :status :body])
      (update :body (fn [body] (condp instance? body
                                 String body
                                 java.io.InputStream (util/input-stream->string body)
                                 (throw (IOException. "Failed to serialize body of type: " (type body))))))))

(nippy/extend-freeze SimpleRecord ::serialize
                     [this os]
                     (.writeUTF os (str (:url this)))
                     (byte-serializer/write-array os (-> this
                                                         :response
                                                         prepare-response
                                                         nippy/freeze)))

(nippy/extend-thaw ::serialize
                   [is]
                   (simple-record (uri/uri (.readUTF is))
                                  (-> is byte-serializer/read-array nippy/thaw)))

(comment
  (require '[clj-http.client :as client])
  (require '[lambdaisland.uri :as uri])
  (def url "https://finnvolkel.com/about")
  (def response (client/get url))

  (def rec (simple-record (uri/uri url) response))
  (simple-record (uri/uri url) (dissoc response :headers))

  (-> rec nippy/freeze nippy/thaw))
