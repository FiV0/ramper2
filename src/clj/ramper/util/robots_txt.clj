(ns ramper.util.robots-txt
  "Functions for parsing robots.txt"
  (:require [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [ramper.url :as url-util]))

(def ramper-agent "ramper-agent")

(defn robots-txt
  "Returns the \"robots.txt\" path of an `url-like` object."
  [uri-like]
  (assoc (url-util/base uri-like)
         :path "/robots.txt"))

(defn- robots-pattern->re [pattern]
  (let [pattern (cond-> pattern
                  (str/ends-with? pattern "?") (subs 0 (dec (count pattern))))]
    (-> pattern
        (str/replace "*" "[^\\s]*")
        (str/replace "?" "\\?")
        re-pattern)))

(defn parse-robots-txt
  "Parses a robot txt as string.

  Returns a map of
  -`:disallow`- the disallowed paths, sorted
  -`:crawl-delay`- the crawl delay in milliseconds if present
  -`:sitemap` - the sitemap url if present"
  ([txt] (parse-robots-txt txt ramper-agent))
  ([txt agent]
   (loop [[line & lines] (str/split txt #"\n")
          res {:disallow [] :sitemap [] :crawl-delay nil}
          relevant false]
     (if line
       (let [[type value] (->> (str/split line #" ") (remove empty?))
             type (when type (str/lower-case type))]
         (cond
           ;; empty line, new block starts
           (empty? line)
           (recur lines res relevant)

           ;; check whether the next block concerns us
           (and (= "user-agent:" type) (or (= "*" value) (= agent value)))
           (recur lines res true)

           ;; append disallows if relevant for us
           (and (= "disallow:" type) relevant)
           (recur lines (update res :disallow conj value) relevant)

           ;; parse crawl delay if relevant for us
           (and (= "crawl-delay:" type) relevant)
           (recur lines (assoc res :crawl-delay (* (Integer/parseInt value) 1000)) relevant)

           ;; add sitemap if present
           (= "sitemap:" type)
           (recur lines (update res :sitemap conj value) relevant)

           :else ;; something else
           (recur lines res relevant)))
       (-> res
           (update :disallow #(map robots-pattern->re %))
           (assoc :timestamp (System/currentTimeMillis)))))))

;; TODO optimisation with streams from http response directly
;; TODO sort prefix free disallow

(comment
  (def hn-txt (slurp (str (robots-txt "https://news.ycombinator.com/foo/"))))
  (def clojure-robots (slurp (str (robots-txt "https://clojure.org"))))
  (def youtube-robots (slurp (str (robots-txt "https://youtube.com"))))
  (def github-robots (slurp (str (robots-txt "https://github.com"))))

  (parse-robots-txt hn-txt)
  (parse-robots-txt clojure-robots)
  (parse-robots-txt youtube-robots)
  (parse-robots-txt github-robots)

  (def other-robot "User-Agent: yolo\nDisallow: /x?\nDisallow: /r?\nDisallow: /vote?\nDisallow: /reply?\nDisallow: /submitted?\nDisallow: /submitlink?\nDisallow: /threads?\nCrawl-delay: 30\n")
  (def other-robot2 "User-agent: ramper-agent # replace 'BadBot' with the actual user-agent of the bot\nUser-agent: Googlebot\nDisallow: /private/")

  (parse-robots-txt other-robot)
  (parse-robots-txt other-robot2)
  )

(defn robots-txt-store
  "Creates a store from base to parsed robots.txt files. See also `parse-robots-txt`."
  []
  {})

(defn add-robots-txt
  "Adds a parsed robots.txt to the store."
  [robots-store base robots-txt]
  (assoc robots-store base robots-txt))

(defn remove-robots-txt
  "Removes the robots.txt from the store."
  [robots-store base]
  (dissoc robots-store base))

(defn crawl-delay
  "Returns the crawl-delay of a site if present."
  [robots-store base]
  (:crawl-delay (get robots-store base)))

(defn disallowed?
  "Returns a truthy value when the url is disallowed by the robots exclusion standard."
  [robots-store url]
  (let [url (uri/uri url)]
    (when-let [entry (get robots-store (url-util/base url))]
      (some #(re-matches % (:path url)) (:disallow entry)))))

(comment
  (def robots-store (atom (robots-txt-store)))

  (swap! robots-store add-robots-txt (url-util/base (uri/uri "https://news.ycombinator.com/foo/")) (parse-robots-txt hn-txt))
  (swap! robots-store add-robots-txt (url-util/base "https://clojure.org") (parse-robots-txt clojure-robots))

  (disallowed? @robots-store "https://clojure.org/search?addsearch=sss")
  (disallowed? @robots-store "https://clojure.org/about/rationale")

  (disallowed? @robots-store "https://news.ycombinator.com/reply?id=29439736&goto=item%3Fid%3D29415518%2329439736")
  (disallowed? @robots-store "https://news.ycombinator.com/foo")

  (dissallowed? @robots-store "https://finnvolkel.com/bla")
  )
