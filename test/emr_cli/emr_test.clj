(ns emr-cli.emr-test
  (:require [clojure.test :refer :all]
            [emr-cli.emr :refer :all]))

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
  (testing "creating emr request"
    (let [params {:name "test" :log-uri "s3://mytestlog" :subnet "subnet-asdf" :instance-type "m4.4xlarge" :key "mykey"
                  :instance-count 5 :bid-pct 50 :job-role "myjobrole" :service-role "servrole" :region "us-east-1"
                  :Tags [{:Key "k1" :Value "v1"} {:Key "k2" :Value "v2"}]}
          request (create-request params)]
      (is (= (:Name request) (:name params)))
      (is (= (:LogUri request) (:log-uri params)))
      (is (= (:JobFlowRole request) (:job-role params)))
      (is (= (:service-role params) (:ServiceRole request)))
      )))