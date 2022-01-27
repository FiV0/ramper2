(ns ramper.parser
  (:require [pantomime.media :as media]
            [pantomime.mime :as mime]))

(def ^:private default-mime-type "application/octet-stream")

(defmulti parse
  "Parses `content` based on content type detection and `content-type`.
  Returns a map of `:content-type , the best guess of the content-type,
  and :parsed-content."
  (fn [content-type content]
    ;; this string is so tika performs better on strings
    (let [detected-type (mime/mime-type-of (cond-> content (string? content) (.getBytes)))]
      (if (= detected-type default-mime-type)
        (or (media/base-type content-type) default-mime-type)
        detected-type))))

(defmethod parse :default [content-type content]
  (throw (ex-info "No method parse found for:" {:content-type content-type
                                                :content content})))

(ns-unmap *ns* 'extract-links)
(ns-unmap *ns* 'extract-text)

(defmulti extract-links
  "Extracts the links of a parsed content. See `parse`."
  (fn [{:keys [content-type]}] content-type))

(defmethod extract-links :default [{:keys [content-type parsed-content]}]
  (throw (ex-info "No method extract-links found for:" {:content-type content-type
                                                        :content parsed-content})))

(defmulti extract-text
  "Extracts the links of a parsed content. See `parse`."
  (fn [{:keys [content-type]}] content-type))

(defmethod extract-text :default [{:keys [content-type parsed-content]}]
  (throw (ex-info "No method extract-text found for:" {:content-type content-type
                                                       :content parsed-content})))
