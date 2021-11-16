(ns ramper.workbench.simple-bench.wrapped-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]
            [ramper.util.url-factory :as url-factory]
            [ramper.workbench :as workbench]
            [ramper.workbench.simple-bench.wrapped :as simple-bench]))

(deftest simple-bench-wrapped-test
  (testing "virtualized bench"
    (let [bench (simple-bench/simple-bench-factory)
          enqueued (atom #{})
          dequeued (atom #{})
          nb-different-bases 2
          nb-items 10000
          _ (dotimes [_ nb-different-bases]
              (async/go-loop [[url & urls] (map str (url-factory/rand-scheme+authority-seq nb-items))]
                (when url
                  (workbench/cons-bench! bench url)
                  (swap! enqueued conj url)
                  (recur urls))))
          _ (dotimes [_ nb-different-bases]
              (loop [cnt 0]
                (when (< cnt nb-items)
                  (if-let [url (workbench/dequeue! bench)]
                    (do
                      (swap! dequeued conj url)
                      (workbench/readd! bench url (- (System/currentTimeMillis) 100))
                      (recur (inc cnt)))
                    (recur cnt)))))]
      (is (= @enqueued @dequeued)))))

(comment
  (time (simple-bench-wrapped-test)))
