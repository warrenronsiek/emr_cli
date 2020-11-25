(ns emr-cli.state
  (:require [duratom.core :refer [duratom]]))

(def emr-state (duratom :local-file :file-path "/tmp/emr-cli" :init {}))

(defn add-cluster [{:keys [cluster-name cluster-id pem-key region]}]
  (swap! emr-state conj {(keyword cluster-id) {:name cluster-name :pem pem-key :region region}}))

; TODO: if you pass a name this will delete multiple clusters. not good.
(defn remove-cluster [identifier]
  (swap! emr-state (filter (fn [[k v]] (not (or (= (keyword identifier) k)
                                                (= (:name v) identifier)))))))

(defn print-clusters [state] (doseq [[k v] (seq @emr-state)]
                               (println (:name v) k (:region v))))

(defn get-cluster-ids [name] (map
                               (fn [[k v]] (str (symbol k)))
                               (filter
                                 (fn [[k v]] (= (:name v) name))
                                 @emr-state)))