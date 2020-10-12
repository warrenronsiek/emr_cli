(ns emr-cli.emr-test
  (:require [clojure.test :refer :all]
            [emr-cli.emr :refer :all]
            [emr-cli.utils :refer [parse-conf]]
            [clojure.java.io :as io]))

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
    (is (instance? String (calculate-bid-price {:instanceType "m4.4xlarge" :region "us-east-1" :bidPct 50})))
    (is (instance? Float (Float/parseFloat (calculate-bid-price {:instanceType "m4.4xlarge" :region "us-east-1" :bidPct 50}))))
    (is (<= 0.1 (Float/parseFloat (calculate-bid-price {:instanceType "m4.4xlarge" :region "us-east-1" :bidPct 50})) 1.0))))

(deftest create-request-test
  (let [params (parse-conf (slurp (io/resource "example_conf.yml")))
        request (create-request params)]
    (testing "top level request arguments"
      (is (= (:Name request) (:name params)))
      (is (= (:LogUri request) (:logUri params)))
      (is (= (:JobFlowRole request) (:jobRole params)))
      (is (= (:ServiceRole request) (:serviceRole params)))
      (is (= (:Tags request) (:tags params))))
    (testing "level 2 nested arguments"
      (is (= (-> request (:Instances) (:Ec2SubnetId))
             (:subnet params)))
      (is (= (-> request (:Instances) (:Ec2KeyName))
             (:key params))))
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
          (is (instance? Float (Float/parseFloat (:BidPrice worker)))))
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
                                  :Market        "ON_DEMAND"}))
            ))))))

;(run-tests)