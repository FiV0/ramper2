(ns ramper.worker.distributor-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [ramper.worker.distributor :as distributor]
            [ramper.sieve :as sieve]
            [ramper.util :as util]))

(deftest sieve-receiver-loop-test
  (testing "spawn-sieve-receiver-loop"
    (let [the-sieve (sieve/create-sieve :memory)
          data (map str (range 10))
          [instance-0-data instance-1-data] ((juxt filter remove) #(util/instance-url? (str %) 0 2) data)
          sieve-receicer (async/chan 10)
          external-chan (async/chan 10)
          sieve-receicer-loop (distributor/spawn-sieve-receiver-loop the-sieve sieve-receicer
                                                                     {:external-chan external-chan
                                                                      :instance-id 0
                                                                      :instance-n 2})]
      (async/onto-chan! sieve-receicer [(take 5 data) (drop 5 data)])
      (is (nil? (async/<!! sieve-receicer-loop)))
      (is (= instance-0-data (repeatedly (count instance-0-data) #(sieve/dequeue! the-sieve))))
      (async/close! external-chan)
      (is (= instance-1-data (->> (async/<!! (async/into [] external-chan))
                                  (mapcat identity)))))))

(comment
  (clojure.test/run-tests)

  )
