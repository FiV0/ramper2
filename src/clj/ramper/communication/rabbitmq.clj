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

(defmethod ig/halt-key! :rabbitmq [_ {:keys [conn channel] :as sys}]
  (log/info :rabbitmq "Stopping rabbitmq component")
  (try
    (when channel (rmq/close channel))
    (when conn (rmq/close conn))
    (catch Exception e
      (log/error :rabbitmq/closing e)))
  (dissoc sys :conn :channel))

(def ^{:const true} ramper-exchange "ramper-exchange")

(defn init-conn []
  (let [conn (rmq/connect)]
    {:conn conn
     :ch (lch/open conn)}))

(defn declare-ramper-exchange [rch]
  (le/declare rch ramper-exchange "topic" {:durable false :auto-delete true}))

(comment
  (do
    (def conn (rmq/connect))
    (def ch (lch/open conn))
    (declare-ramper-exchange ch))
  )

;; TODO parameterize channel size
(defn create-consumer-chan
  "Starts a consumer bound to the given topic exchange in a separate thread"
  [rch routing-key queue-name]
  (let [res-chan (async/chan 100)
        queue-name' (:queue (lq/declare rch queue-name {:exclusive false :auto-delete true}))
        handler     (fn [_ch {:keys [_routing-key _type] :as _meta} ^bytes payload]
                      (async/>!! res-chan (nippy/thaw payload)))]
    (lq/bind rch queue-name' ramper-exchange {:routing-key routing-key})
    (lc/subscribe rch queue-name' handler {:auto-ack true})
    res-chan))

(defn push-update
  "Publishes `payload` to ramper-echange according to `routing-key`."
  ([rch payload routing-key]
   (push-update rch payload routing-key (str "default-" ramper-exchange "-message")))
  ([rch payload routing-key type]
   (lb/publish rch ramper-exchange routing-key (nippy/freeze payload)
               {:content-type "application/octet-stream" :type type})))

(comment
  (def queues [["instance-1-all" "instance-1.#"]
               ["instance-1-urls" "instance-1.urls"]
               ["instance-2-all" "instance-2.#"]])

  (def chan-mapping (->> (map (fn [[queue-name routing-key :as k]]
                                [k (create-consumer-chan ch routing-key queue-name)])
                              queues)
                         (into {})))

  (first chan-mapping)

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

  )
