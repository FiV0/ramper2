(ns ramper.communication.instance
  (:require [ramper.communication.rabbitmq :as rabbitmq]))

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
  (if-let [ch (get @queues [i queue-type])]
    ch
    (let [res (rabbitmq/create-consumer-chan rch
                                             (instance-routing-key i queue-type)
                                             (instance-queue-name i queue-type))]
      (swap! queues assoc [i queue-type] res)
      res)))

(defn push-update [rch payload i queue-type]
  (rabbitmq/push-update rch payload (instance-routing-key i queue-type)))

(comment
  (require '[langohr.core      :as rmq]
           '[langohr.channel   :as lch]
           '[clojure.core.async :as async])

   (def conn (rmq/connect))
   (def rch (lch/open conn))
   (rabbitmq/declare-ramper-exchange rch)

  (push-update rch {:foo :all} 1 :all)

  (-> (get-consumer-chan rch 1 :all) async/poll!)

  )
