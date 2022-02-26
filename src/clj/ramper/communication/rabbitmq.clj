(ns ramper.communication.rabbitmq
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [integrant.core :as ig]
            [langohr.basic     :as lb]
            [langohr.channel   :as lch]
            [langohr.consumers :as lc]
            [langohr.core      :as rmq]
            [langohr.exchange  :as le]
            [langohr.queue     :as lq]
            [taoensso.nippy :as nippy]))

(defmethod ig/init-key :rabbitmq [_ sys]
  (log/info :rabbitmq "Starting rabbitmq component")
  (let [conn (rmq/connect sys)
        ch   (lch/open conn)]
    (assoc sys
           :conn conn
           :channel ch)))

(defmethod ig/halt-key! :rabbitmq [_ {:keys [conn channel]}]
  (log/info :rabbitmq "Stopping rabbitmq component")
  (when conn (rmq/close conn))
  (when channel (rmq/close channel)))

(def ^{:const true} ramper-exchange "ramper-exchange")

(defn init-conn []
  (let [conn (rmq/connect)]
    {:conn conn
     :ch (lch/open conn)}))

(defn declare-ramper-exchange [ch]
  (le/declare ch ramper-exchange "topic" {:durable false :auto-delete true}))

(comment
  (declare-ramper-exchange ch)
  )

(defn create-consumer-chan
  "Starts a consumer bound to the given topic exchange in a separate thread"
  [ch topic-name queue-name]
  (let [res-chan (async/chan 100)
        queue-name' (:queue (lq/declare ch queue-name {:exclusive false :auto-delete true}))
        handler     (fn [ch {:keys [routing-key] :as meta} ^bytes payload]
                      (async/>!! res-chan [routing-key (nippy/thaw payload)]))]
    (lq/bind ch queue-name' ramper-exchange {:routing-key topic-name})
    (lc/subscribe ch queue-name' handler {:auto-ack true})
    res-chan))

(defn push-update
  "Publishes `payload` to ramper-echange according to `routing-key`."
  [ch payload routing-key]
  (lb/publish ch ramper-exchange routing-key (nippy/freeze payload)
              {:content-type "application/octet-stream" :type "weather.update"}))

(def queues [["instance-1-all" "instance-1.#"]
             ["instance-1-urls" "instance-1.urls"]
             ["instance-2-all" "instance-2.#"]])

(def chan-mapping (->> (map (fn [[queue-name topic-name :as k]]
                              [k (create-consumer-chan ch topic-name queue-name)])
                            queues)
                       (into {})))

(comment
  (first chan-mapping))

(push-update ch {:foo :bar} "instance-1")
(push-update ch {:url :bar} "instance-1.urls")

(-> (get chan-mapping ["instance-1-all" "instance-1.#"])
    (async/poll!))

(-> (get chan-mapping  ["instance-1-urls" "instance-1.urls"])
    (async/poll!))


(push-update ch {:foo :bar} "instance-2")

(-> (get chan-mapping ["instance-2-all" "instance-2.#"])
    (async/poll!))


(do
  (rmq/close ch)
  (rmq/close conn))
