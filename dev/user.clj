(ns user
  (:require [clj-async-profiler.core :as prof]
            [clj-async-profiler.ui :as ui]
            [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as repl]
            [criterium.core :as cr]
            [io.pedestal.log :as log]
            [kusonga.move :as move]
            [lambdaisland.classpath.watch-deps :as watch-deps]
            [org.httpkit.client :as client]
            [ramper.util :as util]))

(defn watch-deps! []
  (watch-deps/start! {:aliases [:dev :test]}))

(comment
  ;; refreshing stuff
  (repl/set-refresh-dirs (io/file "src/clj") (io/file "dev"))
  (repl/refresh)
  (repl/clear)

  (watch-deps!)

  (move/move-ns 'ramper.util.robots-txt.wrapped 'ramper.util.robots-store.wrapped (io/file "src/clj") [(io/file "src/clj")])

  ;; profiling
  (prof/start {})

  (def result (prof/stop {}))

  (prof/serve-files 1234)

  (ui/start-server 8080 (io/file "flamegraphs"))

  ;;other
  (def runtime (Runtime/getRuntime))

  (/ (.maxMemory runtime) (* 1024 1024))

  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def problem-url "https://answers.opencv.org/question/15039/creating-a-panorama-from-multiple-images-how-to-reduce-calculation-time/")
  (slurp problem-url)

  (def res @(client/get problem-url #_{:follow-redirects false}))

  )
