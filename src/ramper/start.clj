(ns ramper.start
  (:require [clojure.java.io :as io]
            [ramper.util :as util]))

(def config (atom {}))

(defn start [seed-path]
  (swap! config assoc :ramper/stop false)
  (util/read-urls* seed-path))

(defn stop []
  (swap! config assoc :ramper/stop true))

(comment
  (start (io/file (io/resource "seed.txt")))

  )
