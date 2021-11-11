(ns user
  (:require [clj-async-profiler.core :as prof]
            [clj-async-profiler.ui :as ui]
            [clojure.core.async :as async]
            [clojure.tools.namespace.repl :as repl]
            [clojure.java.io :as io]
            [criterium.core :as cr]
            [io.pedestal.log :as log]
            [org.httpkit.client :as client]
            [ramper.util :as util]))

(comment
  (repl/set-refresh-dirs (io/file "src/clj"))
  (repl/refresh)

  (prof/start {})

  (def result (prof/stop {}))

  (prof/serve-files 1234)

  (ui/start-server 8080 (io/file "flamegraphs"))

  (def runtime (Runtime/getRuntime))

  (/ (.maxMemory runtime) (* 1024 1024))

  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")

  (def problem-url "https://answers.opencv.org/question/15039/creating-a-panorama-from-multiple-images-how-to-reduce-calculation-time/")
  (slurp problem-url)

  (def res @(client/get problem-url #_{:follow-redirects false}))

  )
