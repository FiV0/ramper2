(ns ramper.util.robots-txt
  "Functions for parsing robots.txt"
  (:require [clojure.string :as str]
            [ramper.url :as url-util]))

(def ramper-agent "ramper-agent")

(defn robots-txt
  "Returns the \"robots.txt\" path of an `url-like` object."
  [uri-like]
  (assoc (url-util/base uri-like)
         :path "/robots.txt"))

(defn parse-robots-txt
  "Parses a robot txt as string.

  Returns a map of
  -`:disallow`- the disallowed paths, sorted
  -`:crawl-delay`- the crawl delay if present
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
           (recur lines res false)

           ;; check whether the next block concerns us
           (and (= "user-agent:" type)
                (or (= "*" value) (= agent value)))
           (recur lines res true)

           ;; append disallows if relevant for us
           (and (= "disallow:" type) relevant)
           (recur lines (update res :disallow conj value) relevant)

           ;; parse crawl delay if relevant for us
           (and (= "crawl-delay:" type) relevant)
           (recur lines (assoc res :crawl-delay (Integer/parseInt value)) relevant)

           ;; add sitemap if present
           (= "sitemap:" type)
           (recur lines (update res :sitemap conj value) relevant)

           :else ;; something else
           (recur lines res relevant)))
       ;; TODO check if sort necessary
       (update res :disallow (comp vec sort))))))

;; TODO optimisation with streams from http response directly
;; TODO sort prefix free disallow

(comment
  (def hn-txt (slurp (str (robots-txt "https://news.ycombinator.com/foo/"))))
  (def clojure-robots (slurp (str (robots-txt "https://clojure.org"))))
  (def youtube-robots (slurp (str (robots-txt "https://youtube.com"))))
  hn-txt

  (parse-robots-txt hn-txt)
  (parse-robots-txt clojure-robots)
  (parse-robots-txt youtube-robots)

  (def other-robot "User-Agent: yolo\nDisallow: /x?\nDisallow: /r?\nDisallow: /vote?\nDisallow: /reply?\nDisallow: /submitted?\nDisallow: /submitlink?\nDisallow: /threads?\nCrawl-delay: 30\n")

  (parse-robots-txt other-robot)

  )
