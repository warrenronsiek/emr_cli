(ns emr-cli.utils-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [emr-cli.utils :refer [parse-conf]]))


(deftest conf-consumption
  (testing "configs validate"
    (let [good-conf (parse-conf (slurp (io/resource "example_conf.yml")))
          bad-conf (slurp (io/resource "bad_conf.yml"))
          not-even-wrong-conf "asdf"]
      (is (= (:name good-conf) "test"))
      (is (= (:logUri good-conf) "s3://spark-boilerplate/logs/"))
      (is (= (:subnet good-conf) "subnet-b0711ec9"))
      (is (= (:instanceType good-conf) "m4.xlarge"))
      (is (= (:key good-conf) "warren-laptop") )
      (is (= (:instanceCount good-conf) 1))
      (is (= (:bidPct good-conf) 50))
      (is (= (:serviceRole good-conf) "emr-default-role"))
      (is (= (:instanceRole good-conf) "emr-default-instance-role"))
      (is (= (:region good-conf) "us-east-1"))
      (is (= (:tags good-conf) [{:Key "Testkey1" :Value "TestValue1"}
                                {:Key "Testkey2" :Value "TestValue2"}])))))