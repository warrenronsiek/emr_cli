(ns emr_cli.core
  (:require [cli-matic.core :refer [run-cmd]]
            [emr-cli.emr :refer [create-cluster]]
            [emr-cli.utils :refer [parse-conf]])
  (:gen-class))

(defn hello [& args] (println (str "hello " (map str args))))
(defn cc [{:keys [conf]}] (create-cluster (parse-conf conf)))
(def CONFIGURATION
  {:command "emr-cli"
   :description "creates your emr clusters"
   :version "0.0.1"
   :subcommands [{:command "hi"
                  :short "h"
                  :description ["says hi"]
                  :opts [{:option "name" :short "n" :type :string :default ""}]
                  :runs hello}
                 {:command "create-cluster"
                  :short "cc"
                  :description ["creates cluster"]
                  :opts [{:option "conf" :short "c" :type :slurp}]
                  :runs cc}]})

(defn -main [& args] (run-cmd args CONFIGURATION))