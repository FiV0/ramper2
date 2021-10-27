(ns ramper.util
  (:require [clojure.java.io :as io]
            [lambdaisland.uri :as uri])
  (:import (it.unimi.dsi.bits Fast)
           (java.io InputStream OutputStream PushbackReader Writer)
           (java.nio.file Files)))

(defn read-urls
  "Reads urls as plain strings from a `seed-file`."
  [seed-file]
  (with-open [rdr (io/reader seed-file)]
    (doall (line-seq rdr))))

(defn read-urls*
  "Read urls as lambdaisland.uri.URI's from a `seed-file`."
  [seed-file]
  (->> (read-urls seed-file)
       (map uri/uri)))

(comment
  (-> (io/resource "seed.txt")
      io/file
      read-urls*))

(def runtime (Runtime/getRuntime))

(defn number-of-cores
  "Returns the number of cores available on this machine."
  []
  (.availableProcessors ^java.lang.Runtime runtime))

(defn vbyte-length
  "Returns the length of the vByte encoding of the natural number `x`"
  [^Integer x]
  (inc (/ (Fast/mostSignificantBit x) 7)))

(defn write-vbyte
  "Encodes a natural number `x` (Integer) to an OutputStream `os` using vBytes.
  Returns the number of bytes written."
  [^OutputStream os ^Integer x]
  (cond (zero? (bit-shift-right x 7))
        (do (.write os x) 1)

        (zero? (bit-shift-right x 14))
        (do (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            2)

        (zero? (bit-shift-right x 21))
        (do (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            3)

        (zero? (bit-shift-right x 28))
        (do (.write os (bit-or (unsigned-bit-shift-right x 21) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            4)

        :else
        (do (.write os (bit-or (unsigned-bit-shift-right x 28) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 21) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 14) 0x80))
            (.write os (bit-or (unsigned-bit-shift-right x 7) 0x80))
            (.write os (bit-and x 0x7F))
            5)))

(defn read-vbyte
  "Decodes a natural number from an InputStream `is` using vByte."
  [^InputStream is]
  (loop [x 0]
    (let [b (.read is)
          x (bit-or x (bit-and b 0x7F))]
      (if (zero? (bit-and b 0x80))
        x
        (recur (bit-shift-left x 7))))))

(defn string->bytes [^String s]
  (.getBytes s))

(defn bytes->string [^bytes bs]
  (String. bs))

(comment
  (-> "fooüß" string->bytes bytes->string))
