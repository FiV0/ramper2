(ns ramper
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [integrant.core :as ig]
            [io.pedestal.log :as log]
            [ramper.communication.instance :as commun]
            [ramper.communication.rabbitmq]
            [ramper.instance :as instance]))

;; A dev config for now
(def config {:rabbitmq {:host "localhost", :port 5672}})

(defn read-config [config-path]
  (ig/read-string (slurp config-path)))

(defn start! [config]
  (log/info :ramper/start! "Starting the ramper system ...")
  (ig/init config))

(defn stop! [system]
  (log/info :ramper/start! "Stopping the ramper system ...")
  (ig/halt! system))

(comment
  (require '[integrant.repl :as ig-repl])

  (ig-repl/set-prep! (constantly config))
  (ig-repl/go)
  (ig-repl/halt)

  integrant.repl.state/system

  (alter-var-root #'integrant.repl.state/system (fn [sys] (update sys :rabbitmq dissoc :conn :channel)))

  )

;; TODO think about if all this managing of bootup/shutdown state should also be
;; handled by integrant

(declare stop-one)

(def shutdown-signal :ramper/shutdown)

(defn spawn-shutdown-loop [{:keys [rch instance-data] :as instance} system]
  (let [{:keys [instance-id]} instance-data
        meta-chan (commun/get-consumer-chan rch instance-id :meta)]
    (async/go-loop []
      (if-let [data (async/<! meta-chan)]
        (if (= data shutdown-signal)
          (do
            (async/close! meta-chan)
            (stop-one instance system)
            (log/info :shutdown-loop {:instance-id instance-id
                                      :action :graceful-shutdown}))
          (do
            (log/info :shutdown-loop {:instance-id instance-id
                                      :action :unknown-data
                                      :data data})
            (recur)))
        (log/info :shutdown-loop {:instance-id instance-id
                                  :action :graceful-shutdown})))))

(defn instance [i n config system]
  (log/info :start-instance {:instance-id i :number-of-instances n})
  (let [{:keys [seed-file store-dir]} config
        seed-file (io/file seed-file)
        store-dir (io/file store-dir)
        rch (-> system :rabbitmq :conn)
        external-chan (async/chan 1000)
        incoming-chan (commun/get-consumer-chan rch i :urls)
        instance-data (instance/->InstanceData i n external-chan)
        config (-> config
                   (dissoc :seed-file :store-dir)
                   (assoc :instance-data instance-data))
        {:keys [sieve] :as instance-config} (instance/start seed-file store-dir config)
        outgoing-loop (commun/spawn-outgoing-loop rch external-chan instance-data)
        incoming-loop (commun/spawn-incoming-loop rch sieve instance-data)
        instance {:outgoing-loop outgoing-loop
                  :external-chan external-chan
                  :incoming-loop incoming-loop
                  :incoming-chan incoming-chan
                  :instance-data instance-data
                  :instance-config instance-config
                  :rch rch}
        shutdown-loop (spawn-shutdown-loop instance system)]
    (assoc instance :shutdown-loop shutdown-loop)))

(defn broadcast-shutdown [rch {:keys [instance-id n]}]
  (->> (range n)
       (filter #(= instance-id %))
       (run! #(commun/push-update rch :ramper/shutdown! % :meta))))

(defn stop-one [{:keys [outgoing-loop external-chan incoming-loop incoming-chan
                        instance-config instance-data shutdown-loop rch] :as instance} system]
  (let [{:keys [instance-id]} instance-data
        meta-chan (commun/get-consumer-chan rch instance-id :meta)]
    (instance/stop instance-config)
    (async/close! meta-chan)
    (async/close! external-chan)
    (async/close! incoming-chan)
    (async/<!! shutdown-loop)
    (async/<!! outgoing-loop)
    (async/<!! incoming-loop)))

(defn stop-all [{:keys [instance-data rch] :as instance} system]
  (broadcast-shutdown rch instance-data)
  (stop-one instance system))
