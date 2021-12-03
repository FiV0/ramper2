(ns ramper.util
  (:require [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.constants :as constants])
  (:import (it.unimi.dsi.bits Fast)
           (java.io InputStream OutputStream)
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

(defn write-urls
  "Writes plain string `urls` to a `seed-file`."
  [seed-file urls]
  (with-open [wrt (io/writer seed-file)]
    (doseq [url urls]
      (.write wrt url)
      (.write wrt "\n"))))

(defn write-urls*
  "Writes lambdaisland.uri.URI `urls` to a `seed-file`."
  [seed-file urls]
  (write-urls seed-file (map str urls)))

(def runtime (Runtime/getRuntime))

(defn number-of-cores
  "Returns the number of cores available on this machine."
  []
  (.availableProcessors ^java.lang.Runtime runtime))

(comment
  (number-of-cores)
  )

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

(defn temp-dir
  "Generate a temporary directory in the default temporary-file directory."
  [prefix]
  (-> (Files/createTempDirectory prefix (into-array java.nio.file.attribute.FileAttribute []))
      .toFile))

(defn from-now
  "Returns a timestamp `millis` milliseconds from now."
  [millis]
  (+ (System/currentTimeMillis) millis))

(defn print-time
  "Prints the given timestamp in human readable format."
  [time-ms]
  (cond
    (>= time-ms constants/year)
    (do (print (quot time-ms constants/year) "years ")
        (print-time (mod time-ms constants/year)))

    (>= time-ms constants/day)
    (do (print (quot time-ms constants/day) "days ")
        (print-time (mod time-ms constants/day)))

    (>= time-ms constants/hour)
    (do (print (quot time-ms constants/hour) "hours ")
        (print-time (mod time-ms constants/hour)))


    (>= time-ms constants/minute)
    (do (print (quot time-ms constants/minute) "minutes ")
        (print-time (mod time-ms constants/minute)))

    (>= time-ms constants/sec)
    (do (print (quot time-ms constants/sec) "seconds ")
        (print-time (mod time-ms constants/sec)))

    :else
    (println time-ms "milliseconds")))

(defn multiple-of-8
  "Return the largest multiple of 8 no larger than `x`."
  [x]
  (bit-and x (bit-shift-left -1 3)))

(defn rand-str
  "Returns a random string of length `len` in lower case"
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 97))))))

(defn init-jvm-uncaught-exception-logging []
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/error :uncaught-exception {:ex ex :thread (.getName thread)})))))
