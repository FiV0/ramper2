(ns ramper.util.nippy-extensions
  (:require [com.rpl.nippy-serializable-fn]
            [taoensso.nippy :as nippy])
  (:import (clojure.data.priority_map PersistentPriorityMap)))

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
