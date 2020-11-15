(ns emr-cli.state
  (:require [duratom.core :refer [duratom]]))

(def emr-state (duratom :local-file :file-path "/tmp/emr-cli" :init {}))

(defn add-cluster [{:keys [cluster-name cluster-id pem-key region]}]
  (swap! emr-state conj {(keyword cluster-id) {:name cluster-name :pem pem-key :region region}}))

(defn remove-cluster [identifier]
  (swap! emr-state (filter (fn [[k v]] (not (or (= (keyword identifier) k)
                                                (= (:name v) identifier)))))))

(defn print-clusters [] (map (fn [[k v]] (println (:name v) k (:region v))) @emr-state))