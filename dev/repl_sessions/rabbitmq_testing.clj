(ns repl-sessions.rabbitmq-testing
  (:require [langohr.basic     :as lb]
            [langohr.channel   :as lch]
            [langohr.consumers :as lc]
            [langohr.core      :as rmq]
            [langohr.exchange  :as le]
            [langohr.queue     :as lq]
            [taoensso.nippy :as nippy]
            [clojure.core.async :as async]))

;; one to one
(def ^{:const true} default-exchange-name "")

(defn message-handler
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (println (format "[consumer] Received a message: %s, delivery tag: %d, content type: %s, type: %s"
                   (nippy/thaw payload)  delivery-tag content-type type)))

(def conn (rmq/connect))
(def ch (lch/open conn))
(def qname "ramper-test")

(lq/declare ch qname {:exclusive false :auto-delete true})
(lc/subscribe ch qname message-handler {:auto-ack true})
(lc/subscribe ch qname message-handler {:auto-ack true})

(lb/publish ch default-exchange-name qname (nippy/freeze {:foo 1}) {:content-type "application/octet-stream" :type "greetings.hi"})

(do
  (rmq/close ch)
  (rmq/close conn))

;; many to many
(def ^{:const true} many-exchange "ramper-many")

(defn start-consumer
  "Starts a consumer bound to the given topic exchange in a separate thread"
  [ch topic-name queue-name]
  (let [queue-name' (:queue (lq/declare ch queue-name {:exclusive false :auto-delete true}))
        handler     (fn [ch {:keys [routing-key] :as meta} ^bytes payload]
                      (println (format "[consumer] Consumed '%s' from %s, routing key: %s"
                                       (nippy/thaw payload) queue-name' routing-key)))]
    (lq/bind    ch queue-name' many-exchange {:routing-key topic-name})
    (lc/subscribe ch queue-name' handler {:auto-ack true})))

(defn publish-update
  "Publishes a weather update"
  [ch payload routing-key]
  (lb/publish ch many-exchange routing-key (nippy/freeze payload) {:content-type "application/octet-stream" :type "weather.update"}))

(let [conn      (rmq/connect)
      ch        (lch/open conn)
      locations {""               "americas.north.#"
                 "americas.south" "americas.south.#"
                 "us.california"  "americas.north.us.ca.*"
                 "us.tx.austin"   "#.tx.austin"
                 "it.rome"        "europe.italy.rome"
                 "asia.hk"        "asia.southeast.hk.#"}]
  (le/declare ch many-exchange "topic" {:durable false :auto-delete true})
  (doseq [[k v] locations]
    (start-consumer ch v k))
  (publish-update ch "San Diego update" "americas.north.us.ca.sandiego")
  (publish-update ch "Berkeley update"  "americas.north.us.ca.berkeley")
  (publish-update ch "SF update"        "americas.north.us.ca.sanfrancisco")
  (publish-update ch "NYC update"       "americas.north.us.ny.newyork")
  (publish-update ch "São Paolo update" "americas.south.brazil.saopaolo")
  (publish-update ch "Hong Kong update" "asia.southeast.hk.hongkong")
  (publish-update ch "Kyoto update"     "asia.southeast.japan.kyoto")
  (publish-update ch "Shanghai update"  "asia.southeast.prc.shanghai")
  (publish-update ch "Rome update"      "europe.italy.rome")
  (publish-update ch "Paris update"     "europe.france.paris")
  (Thread/sleep 2000)
  (rmq/close ch)
  (rmq/close conn))

;; many to one ramper

(def ^{:const true} ramper-exchange "ramper-exchange")

(def conn (rmq/connect))
(def ch (lch/open conn))

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
