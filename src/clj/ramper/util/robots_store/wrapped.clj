(ns ramper.util.robots-store.wrapped
  (:require [ramper.util.robots-txt :as robots-txt]))

(defn robots-txt-store [] (atom (robots-txt/robots-txt-store)))

(defn add-robots-txt!
  "Adds a parsed robots.txt to the store."
  [robots-store base robots-txt]
  (swap! robots-store robots-txt/add-robots-txt base robots-txt))

(defn remove-robots-txt!
  "Removes the robots.txt from the store."
  [robots-store base]
  (swap! robots-store robots-txt/remove-robots-txt base))

(defn crawl-delay
  "Returns the crawl-delay of a site if present."
  [robots-store base]
  (robots-txt/crawl-delay @robots-store base))

(defn disallowed?
  "Returns a truthy value when the url is disallowed by the robots exclusion standard."
  [robots-store url]
  (robots-txt/disallowed? @robots-store url))
