(ns emr-cli.emr-test
  (:require [clojure.test :refer :all]
            [emr-cli.emr :refer :all]
            [emr-cli.utils :refer [parse-conf]]
            [clojure.java.io :as io]))

(deftest flat-conj-test
  (testing "flat-conj utility function"
    (let [testcol1 [{:a 1} [{:b 2}]]]
      (is (= (flat-conj testcol1) [{:a 1} {:b 2}])))))

(deftest merge-config-test
  (testing "merging configs"
    (let [testcol1 [{:Classification "a"
                     :Properties {:k 1 :k2 2}}
                    {:Classification "a"
                     :Properties {:k 2}}]
          testcol2 [{:Classification "foo"
                     :Properties     {:k "same"}}
                    {:Classification "foo"
                     :Properties     {:k "same"}}]
          testcol3 [{:Classification "foo"
                     :Properties     {:k "same"}}
                    {:Classification "foo"
                     :Properties     {:k2 "diff"}}]
          testcol4 [{:Classification "bar"
                     :Properties     {:k "same"}}
                    {:Classification "foo"
                     :Properties     {:k "diff"}}]]
      (is (= (merge-configs testcol1) [{:Classification "a"
                                        :Properties     {:k 2 :k2 2}}]))
      (is (= (merge-configs testcol2) [{:Classification "foo"
                                        :Properties     {:k "same"}}]))
      (is (= (merge-configs testcol3) [{:Classification "foo"
                                        :Properties     {:k "same" :k2 "diff"}}]))
      (is (= (merge-configs testcol4) [{:Classification "bar"
                                        :Properties     {:k "same"}}
                                       {:Classification "foo"
                                        :Properties     {:k "diff"}}])))))

(deftest emr-params-test
  (testing "m4.4xlarge params"
    (let [params (calculate-emr-params "m4.4xlarge" 10)]
      (is (= (:executor-cores params) "5"))
      (is (= (:executor-memory params) "18432m"))
      (is (= (:executor-instances params) "29"))
      (is (= (:shuffle-partitions params) "2100"))
      (is (= (:yarn-memory-overhead params) "6"))
      (is (= (:yarn-allocateable-memory-per-node params) "57344"))
      (is (= (:yarn-allocateable-cores-per-node params) "15")))))


(deftest price-calculation-test
  (testing "price calculation"
    (let [params (parse-conf (slurp (io/resource "example_conf.yml")))]
      (is (instance? String (calculate-bid-price params)))
      (is (instance? Float (Float/parseFloat (calculate-bid-price params))))
      (is (<= 0.1 (Float/parseFloat (calculate-bid-price params)) 1.0)))))

(deftest create-request-test
  (let [params (parse-conf (slurp (io/resource "example_conf.yml")))
        request (create-request params)]
    (testing "top level request arguments"
      (is (= (:Name request) (:clusterName params)))
      (is (= (:LogUri request) (:logUri params)))
      (is (= (:JobFlowRole request) (:instanceProfile params)))
      (is (= (:ServiceRole request) (:serviceRole params)))
      (is (= (:Tags request) (:tags params))))
    (testing "level 2 nested arguments"
      (is (= (-> request (:Instances) (:Ec2SubnetId))
             (:subnet params)))
      (is (= (-> request (:Instances) (:Ec2KeyName))
             (:pemKey params)))
      (is (= (-> request (:Instances) (:KeepJobFlowAliveWhenNoSteps))
             (not (some? (:jar params))))))
    (testing "instance groups"
      (let [instances (:InstanceGroups (:Instances request))
            master (first instances)
            worker (second instances)]
        (testing "master group"
          (is (= master {:Name          "master"
                         :InstanceRole  "MASTER"
                         :Market        "ON_DEMAND"
                         :InstanceType  (:instanceType params)
                         :InstanceCount 1})))
        (testing "worker group"
          (is (= (:Market worker) "SPOT"))
          (is (= (:InstanceType worker) (:instanceType params)))
          (is (= (:InstanceCount worker) (:instanceCount params)))
          (is (instance? Float (Float/parseFloat (:BidPrice worker)))))))))

(deftest cluster-config-request
  (testing "cluster configuration props"
    (let [params (parse-conf (slurp (io/resource "example_conf.yml")))
          request (create-request params)
          emr-params (calculate-emr-params (:instanceType params) (:instanceCount params))
          config (:Configurations request)
          spark-defaults (:Properties (first config))
          yarn-site (:Properties (second config))]
      (is (= (:spark.driver.memory spark-defaults) (:executor-memory emr-params)))
      (is (= (:spark.driver.cores spark-defaults) (:executor-cores emr-params)))
      (is (= (:spark.executor.memory spark-defaults) (:executor-memory emr-params)))
      (is (= (:spark.executor.instances spark-defaults) (:executor-instances emr-params)))
      (is (= (:spark.executor.cores spark-defaults) (:executor-cores emr-params)))
      (is (= (:spark.sql.shuffle.partitions spark-defaults) (:shuffle-partitions emr-params)))
      (is (= (:spark.executor.memoryOverhead spark-defaults) (:yarn-memory-overhead emr-params)))
      (is (= (:yarn.nodemanager.resource.memory-mb yarn-site) (:yarn-allocateable-memory-per-node emr-params)))
      (is (= (:yarn.nodemanager.resource.cpu-vcores yarn-site) (:yarn-allocateable-cores-per-node emr-params)))
      (is (= (first (filter (fn [classification] (= (:Classification classification) "emrfs-site")) config))
             {:Classification "emrfs-site"
              :Properties     {:fs.s3.authorization.roleMapping "my-other-iam-role"}})))))

(deftest on-demand-request
  (testing "worker group on demand"
    (let [demand-params (parse-conf (slurp (io/resource "example_conf_ondemand.yml")))
          demand-worker (-> demand-params
                            (create-request)
                            (:Instances)
                            (:InstanceGroups)
                            (second))]
      (is (= demand-worker {:Name          "worker"
                            :InstanceRole  "CORE"
                            :InstanceType  (:instanceType demand-params)
                            :InstanceCount (:instanceCount demand-params)
                            :Market        "ON_DEMAND"})))))