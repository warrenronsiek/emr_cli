(ns emr-cli.emr
  (:require [cognitect.aws.client.api :as aws]
            [emr-cli.utils.conf-parse :refer [ec2-info]]
            [emr-cli.utils :as utils]
            [clojure.core.async :refer [thread]]
    ; [clj-ssh.ssh :refer [ssh-agent session forward-remote-port]]
            [emr-cli.state :refer [add-cluster remove-cluster get-cluster-ids]]
            [taoensso.timbre :as timbre :refer [info debug spy]]
            [clojure.core :refer [<= >= < >]]
            [clojure.java.io :as io]
            [clj-yaml.core :as yaml]
            [clojure.core.reducers :as r]
            [clojure.string :as str]))

(defn calculate-bid-price [config]
  (let [pricing (utils/client-builder config "pricing" "us-east-1")
        endpoints (yaml/parse-string (slurp (io/resource "endpoints.json")))
        get-region-name (fn [region-code] (:description ((keyword region-code) (:regions
                                                                                 (first (:partitions endpoints))))))
        filters [{:Field "tenancy" :Value "shared" :Type "TERM_MATCH"}
                 {:Field "operatingSystem" :Value "Linux" :Type "TERM_MATCH"}
                 {:Field "preInstalledSw" :Value "NA" :Type "TERM_MATCH"}
                 {:Field "instanceType" :Value (:instanceType config) :Type "TERM_MATCH"}
                 {:Field "location" :Value (get-region-name (utils/get-region config)) :Type "TERM_MATCH"}
                 {:Field "capacitystatus" :Value "Used" :Type "TERM_MATCH"}]
        data (aws/invoke pricing {:op :GetProducts :request {:ServiceCode "AmazonEC2" :Filters filters}})
        on-demand (:OnDemand (:terms (yaml/parse-string (first (:PriceList data)))))
        price-string (:USD (:pricePerUnit (second (first (:priceDimensions (second (first on-demand)))))))]
    (format "%.2f" (* (Float/parseFloat price-string) (/ (:bidPct config) 100)))))

(defn calculate-emr-params [instance-type worker-count]
  ; I am doing a variant of what is specified in some spark books/talks, but you can find something similar in:
  ; https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
  (let [memory-overhead-multiplier 0.9                      ; multiplier is to give the os/system memory
        instance ((keyword instance-type) ec2-info)
        allocateable-cores-per-node (- (:cores instance) 1) ; -1 for the yarn nodemanager that has to run on each node
        total-nodes worker-count
        total-cores (* allocateable-cores-per-node total-nodes)
        allocateable-memory-per-node (- (:memory instance) 1) ; -1gb for nodemanager
        total-allocateable-memory (* allocateable-memory-per-node total-nodes memory-overhead-multiplier)
        allocateable-memory-per-node (int (Math/floor (* allocateable-memory-per-node memory-overhead-multiplier)))
        cores-per-executor (reduce (fn [a b]
                                     (cond
                                       (and (= (mod allocateable-cores-per-node a) 0)
                                            (>= allocateable-cores-per-node a)) a
                                       (> (mod allocateable-cores-per-node a) (mod allocateable-cores-per-node b)) b
                                       :else a))
                                   [7 6 5 4 3])
        num-executors (- (int (* (Math/floor (/ allocateable-cores-per-node cores-per-executor)) total-nodes)) 1) ;-1 for the driver
        mem-per-executor (int (Math/floor (/ total-allocateable-memory (+ num-executors 1)))) ;+1 for driver
        shuffle-parallelism (loop [multiple 1]
                              (let [parallelism (* total-cores multiple)]
                                (if (> parallelism 2000) parallelism
                                                         (recur (+ multiple 1)))))]
    {:executor-memory                   (str (* mem-per-executor 1024) "m")
     :executor-cores                    (str cores-per-executor)
     :executor-instances                (str num-executors)
     :shuffle-partitions                (str shuffle-parallelism)
     :yarn-memory-overhead              (str (int (Math/ceil (* allocateable-memory-per-node
                                                                (- 1 memory-overhead-multiplier)))))
     :yarn-allocateable-memory-per-node (str (* allocateable-memory-per-node 1024))
     :yarn-allocateable-cores-per-node  (str allocateable-cores-per-node)}))

(defn flat-conj [& args] (filter some? (flatten (conj [] args))))

(defn merge-configs [config-arr]
  (r/fold
    (r/monoid (fn [ret v] (let [{class :Classification props :Properties} v
                                existing-class (first (filter #(= class (:Classification %)) ret))]
                            (if (some? existing-class)
                              (conj (filter #(not= class (:Classification %)) ret)
                                    {:Classification class
                                     :Properties     (reduce merge {} [(:Properties existing-class) props])})
                              (conj ret v))))
              (constantly []))
    config-arr))

(defn create-request [config]
  "tags of shape {:Key key :Value value}"
  (let [params (calculate-emr-params (:instanceType config) (:instanceCount config))]
    {:Name              (:clusterName config)
     :LogUri            (:logUri config)
     :ReleaseLabel      (str "emr-" (or (:emrVersion config) "6.3.0"))
     :VisibleToAllUsers true
     :JobFlowRole       (:instanceProfile config)
     :ServiceRole       (:serviceRole config)
     :Applications      [{:Name "Spark"} {:Name "Hadoop"} {:Name "Hive"} {:Name "Zeppelin"}]
     :Tags              (:tags config)
     :Instances         (merge
                          {:Ec2SubnetId                 (:subnet config)
                           :Ec2KeyName                  (:pemKey config)
                           :KeepJobFlowAliveWhenNoSteps (not (some? (:jar config)))
                           :TerminationProtected        false
                           :InstanceGroups              [{:Name          "master"
                                                          :InstanceRole  "MASTER"
                                                          :Market        "ON_DEMAND"
                                                          :InstanceType  (:instanceType config)
                                                          :InstanceCount 1}
                                                         (if (:bidPct config) {:Name          "worker"
                                                                               :InstanceRole  "CORE"
                                                                               :Market        "SPOT"
                                                                               :InstanceType  (:instanceType config)
                                                                               :InstanceCount (:instanceCount config)
                                                                               :BidPrice      (calculate-bid-price config)}
                                                                              {:Name          "worker"
                                                                               :InstanceRole  "CORE"
                                                                               :InstanceType  (:instanceType config)
                                                                               :InstanceCount (:instanceCount config)
                                                                               :Market        "ON_DEMAND"})]}
                          (when-let [sec-groups (:additionalSecurityGroups config)]
                            {:AdditionalMasterSecurityGroups (flatten [sec-groups])
                             :AdditionalSlaveSecurityGroups  (flatten [sec-groups])}))
     :Configurations    (merge-configs
                          (flat-conj
                            [{:Classification "spark-defaults"
                              :Properties     {:spark.driver.memory           (:executor-memory params)
                                               :spark.driver.cores            (:executor-cores params)
                                               :spark.executor.memory         (:executor-memory params)
                                               :spark.executor.instances      (:executor-instances params)
                                               :spark.executor.cores          (:executor-cores params)
                                               :spark.sql.shuffle.partitions  (str (or (:shufflePartitions config)
                                                                                       (:shuffle-partitions params)))
                                               :spark.executor.memoryOverhead (:yarn-memory-overhead params)}}
                             {:Classification "yarn-site"
                              :Properties     {:yarn.nodemanager.resource.memory-mb  (:yarn-allocateable-memory-per-node params)
                                               :yarn.nodemanager.resource.cpu-vcores (:yarn-allocateable-cores-per-node params)}}
                             {:Classification "capacity-scheduler"
                              :Properties     {:yarn.scheduler.capacity.resource-calculator "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"}}
                             (if (:configurations config)
                               (map (fn [conf] {:Classification (:classification conf)
                                                :Properties     (reduce
                                                                  #(assoc %1 (keyword (:key %2)) (:value %2))
                                                                  {}
                                                                  (:properties conf))})
                                    (:configurations config)))]))
     :Steps             (flat-conj
                          {:Name            "setup hadoop debugging"
                           :ActionOnFailure "TERMINATE_CLUSTER"
                           :HadoopJarStep   {:Jar  "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar"
                                             :Args ["s3://us-east-1.elasticmapreduce/libs/state-pusher/0.1/fetch"]}}
                          (if (:jar config)
                            (let [jarName (str "/home/hadoop/" (last (str/split (:jar config) #"/")))]
                              [{:Name            "copy jar"
                                :ActionOnFailure "TERMINATE_CLUSTER"
                                :HadoopJarStep   {:Jar  "command-runner.jar"
                                                  :Args ["aws" "s3" "cp" (:jar config) jarName]}}
                               {:Name            "run jar"
                                :ActionOnFailure "TERMINATE_CLUSTER"
                                :HadoopJarStep   {:Jar  "command-runner.jar"
                                                  :Args (flat-conj
                                                          "spark-submit"
                                                          (if (:jarClass config) ["--class" (:jarClass config)])
                                                          "--master" "yarn"
                                                          "--deploy-mode" "cluster"
                                                          jarName
                                                          (map second (:jarArgs config)))}}])))}))

(defn create-cluster [conf]
  (let [emr (utils/client-builder conf "emr")
        request (create-request conf)
        resp (aws/invoke emr {:op :RunJobFlow :request request})]
    (do
      (add-cluster {:cluster-name (:clusterName conf) :cluster-id (:JobFlowId resp) :pem-key (:pemKey conf)})
      (timbre/info resp))))

(defn terminate-clusters
  [conf cluster-ids region]
  (let [emr (if conf (utils/client-builder conf "emr")
                     (utils/client-builder {:region region} "emr"))]
    (aws/invoke emr {:op :TerminateJobFlows :request {:JobFlowIds cluster-ids}})))

;(defn ssh-tunnel [conf cluster-id region]
;  (let [app-port-map {:Zeppelin 8890 :Spark 18080 :Yarn 8088}
;        emr (utils/client-builder conf "emr")
;        cluster-description (aws/invoke emr
;                                        {:op      :DescribeCluster
;                                         :request {:ClusterId (or cluster-id (get-cluster-ids (:clusterName conf)))}})
;        master-dns (:MasterPublicDnsName (:Cluster cluster-description))
;        apps (map :Name (:Applications (:Cluster cluster-description)))]
;    (doseq [app apps]
;      (let [app-port ((keyword app) app-port-map)]
;        (thread (forward-remote-port (session (ssh-agent {}) master-dns {:username      "ec2-user"
;                                                                         :identity_file (:pemKey conf)})
;                                     app-port
;                                     app-port))
;        (println (str "serving " app " on port " app-port))))))