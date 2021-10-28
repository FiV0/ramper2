(ns user
  (:require
   [clojure.java.io :as io]
   [clojure.core.async :as async]
   [org.httpkit.client :as client]
   [ramper.util :as util]))

(comment
  (realized? (client/get "https://hckrnews.com"))
  )

(defn blocking-operation [arg] arg)

(let [concurrent 10
      output-chan (async/chan)
      input-coll (range 0 1000)]
  (async/pipeline-blocking concurrent
                           output-chan
                           (map blocking-operation)
                           (async/to-chan input-coll))
  (async/<!! (async/into [] output-chan)))


(comment
  (def urls (util/read-urls (io/file (io/resource "seed.txt")))))

(defn get-urls [urls]
  (let [concurrent 24
        output-chan (async/chan 1000)
        input-coll urls]
    (async/pipeline-blocking concurrent
                             output-chan
                             (map #(client/get % {:follow-redirects false}))
                             (async/to-chan input-coll))
    (async/<!! (async/into [] output-chan))))

(comment





  (get-urls (take 1 urls))

  @(client/get (first urls))

  (slurp "https://answers.opencv.org/question/15039/creating-a-panorama-from-multiple-images-how-to-reduce-calculation-time/")

  (take 10 urls)

  (def resp (time (mapv deref (get-urls urls))))

  (def res @(client/get (nth urls 9) {:follow-redirects false}))

  @(client/get (nth urls 9) {:follow-redirects true :max-redirects 5 :trace-redirects false})

  (keys res)



  (def res2 @(client/get (first urls)))

  (keys res2)

  res2

  (System/setProperty "clojure.core.async.pool-size" "32")
  (System/getProperty "clojure.core.async.pool-size")


  )
