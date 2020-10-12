(ns emr-cli.emr-test
  (:require [clojure.test :refer :all]
            [emr-cli.emr :refer :all]
            [emr-cli.utils :refer [parse-conf]]
            [clojure.java.io :as io]))

(deftest emr-params-test
  (testing "m4.4xlarge"
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
    (is (instance? String (calculate-bid-price {:instance-type "m4.4xlarge" :region "us-east-1" :bidPct 50})))
    (is (instance? Float (Float/parseFloat (calculate-bid-price {:instance-type "m4.4xlarge" :region "us-east-1" :bidPct 50 }))))
    (is (<= 0.1 (Float/parseFloat (calculate-bid-price {:instance-type "m4.4xlarge" :region "us-east-1" :bidPct 50 })) 1.0))))

(deftest create-request-test
  (testing "top level request arguments"
    (let [params (parse-conf (slurp (io/resource "example_conf.yml")))
          request (create-request params)]
      (is (= (:Name request) (:name params)))
      (is (= (:LogUri request) (:log-uri params)))
      (is (= (:JobFlowRole request) (:job-role params)))
      (is (= (:ServiceRole request) (:service-role params))))))