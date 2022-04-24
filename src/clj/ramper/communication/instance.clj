(ns ramper.communication.instance
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [ramper.communication.rabbitmq :as rabbitmq]
            [ramper.sieve :as sieve]
            [ramper.util :as util]
            [ramper.util.macros :refer [cond-let]]))

;; TODO the main question to ask here is, if the extra indirection (channel
;; on top of rabbitmq) is necessary

(def queue-types #{:all :urls :meta})

(defn- instance-queue-name [i queue-type]
  (if (queue-types queue-type)
    (str "instance-" i "-" (name queue-type))
    (throw (IllegalArgumentException. (str "No such queue-type " queue-type)))))

(comment
  (instance-queue-name 1 :all)
  (instance-queue-name 1 :foo)
  )

(defn- instance-routing-key [i queue-type]
  (case queue-type
    (:urls :meta) (str "instance-" i "." (name queue-type))
    :all (str "instance-" i ".#")
    (throw (IllegalArgumentException. (str "No such queue-type " queue-type)))))

(comment
  (instance-routing-key 1 :all)
  (instance-routing-key 1 :urls)
  (instance-routing-key 1 :foo)
  )

(def queues (atom {}))

(defn get-consumer-chan [rch i queue-type]
  {:pre [(int? i) (queue-types queue-type)]}
  (if-let [ch (get @queues [i queue-type])]
    ch
    (let [res (rabbitmq/create-consumer-chan rch
                                             (instance-routing-key i queue-type)
                                             (instance-queue-name i queue-type))]
      (swap! queues assoc [i queue-type] res)
      res)))

(defn push-update [rch payload i queue-type]
  {:pre [(int? i) (queue-types queue-type)]}
  (rabbitmq/push-update rch payload (instance-routing-key i queue-type)))

(comment
  (require '[langohr.core      :as rmq]
           '[langohr.channel   :as lch]
           '[clojure.core.async :as async])

  (def conn (rmq/connect {:host "localhost", :port 5672}))
  (def rch (lch/open conn))
  (rabbitmq/declare-ramper-exchange rch)

  (def rch2 (-> integrant.repl.state/system :rabbitmq :channel))
  (rabbitmq/declare-ramper-exchange rch2)

  (push-update rch {:foo :all} 1 :all)
  (push-update rch {:foo :urls} 1 :urls)
  (-> (get-consumer-chan rch 1 :all) async/poll!)
  (-> (get-consumer-chan rch 1 :urls) async/poll!)

  (push-update rch2 {:foo :all} 1 :all)
  (-> (get-consumer-chan rch2 1 :all) async/poll!)

  )

;; TODO: rename external to output
(defn spawn-outgoing-loop [rch external-url-chan {:keys [instance-id n]}]
  (async/go-loop []
    (if-let [urls (async/<! external-url-chan)]
      (let [grouped  (group-by #(util/url->bucket % n) urls)]
        (assert (nil? (get grouped instance-id)) (format "Outgoing loop contains urls of instance %d" i))
        (run! (fn [[i urls]] (push-update rch urls i :urls)) grouped)
        (recur))
      (log/info :outgoing-loop :graceful-shutdown))))

(def ^:private accumulated-threshold 100)

(defn spawn-incoming-loop [rch the-sieve {:keys [instance-id n]}]
  (let [incoming-chan (get-consumer-chan rch instance-id :urls)]
    (async/go-loop [accumulated []]
      (cond-let
        (<= accumulated-threshold (count accumulated))
        (do
          (sieve/enqueue*! the-sieve accumulated)
          (recur []))

        [urls (async/<! incoming-chan)]
        ;; TODO more efficient vector concat
        (recur (into accumulated urls))

        :else
        (log/info :incoming-loop :graceful-shutdown)))))
