(ns emr_cli.core
  (:require [cli-matic.core :refer [run-cmd]]
            [emr-cli.emr :refer [create-cluster]]
            [emr-cli.utils :refer [parse-conf]])
  (:gen-class))

(defn hello [{:keys [name]}] (println (str "Hi! " name)))
(defn print-args [{:keys [conf]}] (println (str conf)))

(def CONFIGURATION
  {:app {:command "mlship"
         :description "ships your ml!"
         :version "0.0.1"}
   :commands [{:command "hi"
               :short "h"
               :description ["says hi"]
               :opts [{:option "name" :short "n" :type :string :default ""}]
               :runs hello}
              {:command "create-cluster"
               :short "cc"
               :description ["creates cluster"]
               :opts [{:option "conf" :short "c" :type :slurp}]
               :runs (fn [{:keys conf}] (create-cluster (parse-conf conf)))}]
   })

(defn -main [& args] (run-cmd args CONFIGURATION))