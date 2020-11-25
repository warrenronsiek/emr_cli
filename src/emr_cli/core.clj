(ns emr_cli.core
  (:require [cli-matic.core :refer [run-cmd]]
            [emr-cli.emr :refer [create-cluster terminate-clusters]]
            [emr-cli.utils :refer [parse-conf]]
            [emr-cli.state :refer [print-clusters]])
  (:gen-class))

(defn ^:private !null= [a b] (and (= a b) (not (nil? (and a b)))))

(defn create-cluster-shim [{:keys [conf]}] (create-cluster (parse-conf conf)))

(defn print-clusters-shim [{:keys [state]}] (print-clusters state))

(defn terminate-cluster-shim [{:keys [conf name id region]}]
  (let [clusters (filter (fn [[k v]] (or (!null= k id)
                                         (!null= (:name v) name)
                                         (!null= (:clusterName conf) name)))
                         (print-clusters))
        cluster-ids (map (fn [[k v]] k) clusters)]
    (terminate-clusters conf cluster-ids region)))

(def CONFIGURATION
  {:command     "emr-cli"
   :description "creates your emr clusters"
   :version     "0.0.1"
   :subcommands [{:command     "create-cluster"
                  :short       "cc"
                  :description ["creates cluster"]
                  :opts        [{:option "conf" :short "c" :type :slurp}]
                  :runs        create-cluster-shim}
                 {:command     "list-clusters"
                  :short       "ls"
                  :description ["lists all created clusters"]
                  :opts        [{:option "state" :short "s" :type :string :default "running"}]
                  :runs        print-clusters-shim}
                 {:command     "terminate-cluster"
                  :short       "t"
                  :description ["deletes a cluster"]
                  :opts        [{:option "conf" :short "c" :type :slurp}
                                {:option "name" :short "n" :type :string}
                                {:option "id" :short "i" :type :string}
                                {:option "region" :short "r" :type :string}]
                  :runs        terminate-cluster-shim}]})

(defn -main [& args] (run-cmd args CONFIGURATION))