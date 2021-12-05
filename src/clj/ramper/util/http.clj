(ns ramper.util.http
  "Some utility functions for the http protocol")

(defn informational-response? [code]
  (<= 100 code 199))

(defn successful-response? [code]
  (<= 200 code 299))

(defn redirection-response? [code]
  (<= 300 code 399))

(defn client-error? [code]
  (<= 400 code 499))

(defn server-error? [code]
  (<= 500 code 599))
