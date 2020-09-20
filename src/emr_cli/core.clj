(ns emr_cli.core
  (:require [cli-matic.core :refer [run-cmd]])
  (:gen-class))

(defn hello [{:keys [name]}] (println (str "Hi! " name)))

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
               :opts [{:option "conf" :short "c" :type :slurp}]}]})

(defn -main [& args] (run-cmd args CONFIGURATION))