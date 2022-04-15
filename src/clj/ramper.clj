(ns ramper
  (:require [integrant.core :as ig]
            [io.pedestal.log :as log]
            [ramper.communication.rabbitmq]))

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
