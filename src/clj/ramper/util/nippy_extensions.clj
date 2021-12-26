(ns ramper.util.nippy-extensions
  "A namespace that extends nippy to types that it does not know about."
  (:require [clojure.data.priority-map]
            [clojure.java.io :as io]
            [com.rpl.nippy-serializable-fn]
            [ramper.util.data-disk-queues]
            [ramper.workbench.simple-bench.wrapped]
            [ramper.workbench.virtualized-bench.wrapped :refer [get-bench]]
            [taoensso.nippy :as nippy])
  (:import (clojure.data.priority_map PersistentPriorityMap)
           (java.io ObjectInputStream ObjectOutputStream)
           (ramper.util ByteArrayDiskQueues)
           (ramper.util.data_disk_queues DataDiskQueues)
           (ramper.workbench.simple_bench.wrapped SimpleBench)
           (ramper.workbench.virtualized_bench.wrapped VirtualizedBench)))

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
 SimpleBench ::simple-bench
 [sb data-output]
 (nippy/freeze-to-out! data-output (deref (.bench_atom sb))))

(nippy/extend-thaw
 ::simple-bench
 [data-input]
 (SimpleBench. (atom (nippy/thaw-from-in! data-input))))


;; FIXME this is a big hack and won't work if files are not also copied
(nippy/extend-freeze
 ByteArrayDiskQueues ::byte-array-disk-queues
 [badq data-output]
 (nippy/freeze-to-out! data-output (.getDir badq))
 (.writeLong data-output (. badq -size))
 (.writeLong data-output (. badq -appendPointer))
 (.writeLong data-output (. badq -used))
 (.writeLong data-output (. badq -allocated))
 (.writeInt data-output (.. badq -buffers size))
 (.writeInt data-output (.. badq -key2QueueData size))
 (doseq [entry (.. badq key2QueueData object2ObjectEntrySet)]
   (let [key (.getKey entry)
         queue-data (.getValue entry)]
     (nippy/freeze-to-out! data-output key)
     (.writeObject (ObjectOutputStream. data-output) queue-data))))

(nippy/extend-thaw
 ::byte-array-disk-queues
 [data-input]
 (let [badq (ByteArrayDiskQueues. (nippy/thaw-from-in! data-input))]
   (set! (. badq -size) (.readLong data-input))
   (set! (. badq -appendPointer) (.readLong data-input))
   (set! (. badq -used) (.readLong data-input))
   (set! (. badq -allocated) (.readLong data-input))
   (let [n (.readInt data-input)]
     (.. badq buffers (size n))
     (.. badq files (size n)))
   (dotimes [_ (.readInt data-input)]
     (.put (. badq key2QueueData) (nippy/thaw-from-in! data-input) (.readObject (ObjectInputStream. data-input))))
   badq))

(nippy/extend-freeze
 DataDiskQueues ::data-disk-queues
 [ddq data-output]
 (nippy/freeze-to-out! data-output (:disk-queues ddq))
 (nippy/freeze-to-out! data-output (:directory ddq))
 (nippy/freeze-to-out! data-output (:data->bytes ddq))
 (nippy/freeze-to-out! data-output (:bytes->data ddq)))

(nippy/extend-thaw
 ::data-disk-queues
 [data-input]
 (DataDiskQueues. (nippy/thaw-from-in! data-input)
                  (nippy/thaw-from-in! data-input)
                  (nippy/thaw-from-in! data-input)
                  (nippy/thaw-from-in! data-input)))

(nippy/extend-freeze
 VirtualizedBench ::virtualized-bench
 [vbench data-output]
 (nippy/freeze-to-out! data-output (get-bench vbench)))

(nippy/extend-thaw
 ::virtualized-bench
 [data-input]
 (VirtualizedBench. (nippy/thaw-from-in! data-input)))


(comment
  (require '[ramper.workbench :refer [cons-bench! dequeue! readd!]]
           '[ramper.workbench.virtualized-bench.wrapped :as vbench])
  (def bench (vbench/virtualized-bench-factory))

  (binding [ramper.workbench.virtualized-bench/max-per-key 2]
    (cons-bench! bench "https://finnvolkel.com")
    (cons-bench! bench "https://hckrnews.com")
    (cons-bench! bench "https://finnvolkel.com/about")
    (cons-bench! bench "https://finnvolkel.com/tech"))

  (def bench2 (nippy/thaw (nippy/freeze bench)))

  (binding [ramper.workbench.virtualized-bench/max-per-key 2]
    (dequeue! bench2))

  (binding [ramper.workbench.virtualized-bench/max-per-key 2]
    (readd! bench2 *3 (- (System/currentTimeMillis) 100)))

  (count bench))
