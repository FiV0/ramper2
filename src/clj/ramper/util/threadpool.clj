(ns ramper.util.threadpool
  (:import [java.util.concurrent ThreadPoolExecutor LinkedBlockingQueue TimeUnit]
           [org.httpkit PrefixThreadFactory ]))

(defn create-threadpool [min-core max-core & {:keys [keep-alive prefix]
                                              :or {keep-alive 60 prefix "ramper-pool"}}]
  (let [queue (LinkedBlockingQueue.)
        factory (PrefixThreadFactory. prefix)]
    (ThreadPoolExecutor. min-core max-core keep-alive TimeUnit/SECONDS queue factory)))
