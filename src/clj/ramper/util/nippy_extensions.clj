(ns ramper.util.nippy-extensions
  "A namespace that extends nippy to types that it does not know about."
  (:require [clojure.data.priority-map]
            [clojure.java.io :as io]
            [com.rpl.nippy-serializable-fn]
            [ramper.util.data-disk-queues :as ddq]
            [ramper.workbench.simple-bench.wrapped]
            [taoensso.nippy :as nippy])
  (:import (clojure.data.priority_map PersistentPriorityMap)
           (ramper.util.data_disk_queues DataDiskQueues)
           (ramper.workbench.simple_bench.wrapped SimpleBench)))

(nippy/extend-freeze
 java.io.File ::java-file
 [f data-output]
 (nippy/freeze-to-out! data-output (.getPath f)))

(nippy/extend-thaw
 ::java-file
 [data-input]
 (io/file (nippy/thaw-from-in! data-input)))

(comment
  (nippy/thaw (nippy/freeze (io/file "resources"))))

(nippy/extend-freeze
 PersistentPriorityMap ::priority-map
 [pm data-output]
 (nippy/freeze-to-out! data-output (.priority->set-of-items pm))
 (nippy/freeze-to-out! data-output (.item->priority pm))
 (nippy/freeze-to-out! data-output (._meta pm))
 (nippy/freeze-to-out! data-output (.keyfn pm)))

(nippy/extend-thaw
 ::priority-map
 [data-input]
 (let [priority->set-of-items (nippy/thaw-from-in! data-input)
       item->priority (nippy/thaw-from-in! data-input)
       meta (nippy/thaw-from-in! data-input)
       keyfn (nippy/thaw-from-in! data-input)]
   (PersistentPriorityMap. priority->set-of-items item->priority meta keyfn)))

(comment
  (require '[clojure.data.priority-map :as pm])
  (nippy/thaw (nippy/freeze (pm/priority-map-keyfn :a 1 {:a 1} 2 {:a 2}))))

(nippy/extend-freeze
 DataDiskQueues ::data-disk-queues
 [ddq data-output]
 (nippy/freeze-to-out! data-output (:directory ddq))
 (nippy/freeze-to-out! data-output (:data->bytes ddq))
 (nippy/freeze-to-out! data-output (:bytes->data ddq)))

(nippy/extend-thaw
 ::data-disk-queues
 [data-input]
 (ddq/data-disk-queues (nippy/thaw-from-in! data-input)
                       {:data->bytes (nippy/thaw-from-in! data-input)
                        :bytes->data (nippy/thaw-from-in! data-input)}))

(nippy/extend-freeze
 SimpleBench ::simple-bench
 [sb data-output]
 (nippy/freeze-to-out! data-output (deref (.bench_atom sb))))

(nippy/extend-thaw
 ::simple-bench
 [data-input]
 (SimpleBench. (atom (nippy/thaw-from-in! data-input))))
