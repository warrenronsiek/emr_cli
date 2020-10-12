(ns emr-cli.utils-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [emr-cli.utils :refer [parse-conf client-builder]]))


(deftest good-conf-consumption
  (testing "configs validate"
    (let [good-conf (parse-conf (slurp (io/resource "example_conf.yml")))]
      (is (= (:name good-conf) "test"))
      (is (= (:logUri good-conf) "s3://spark-boilerplate/logs/"))
      (is (= (:subnet good-conf) "subnet-b0711ec9"))
      (is (= (:instanceType good-conf) "m4.4xlarge"))
      (is (= (:key good-conf) "warren-laptop"))
      (is (= (:instanceCount good-conf) 1))
      (is (= (:bidPct good-conf) 50))
      (is (= (:serviceRole good-conf) "emr-default-role"))
      (is (= (:instanceRole good-conf) "emr-default-instance-role"))
      (is (= (:region good-conf) "us-east-1"))
      (is (= (:tags good-conf) [{:Key "Testkey1" :Value "TestValue1"}
                                {:Key "Testkey2" :Value "TestValue2"}])))))

(deftest bad-conf-consumption
  (testing "bad conf validation"
    (let [bad-conf (slurp (io/resource "bad_conf.yml"))]
      (is (thrown-with-msg? java.lang.Exception #"config validation failed" (parse-conf bad-conf))))))

(deftest crazy-conf-consumption
  (testing "not even wrong conf"
    (is (thrown? java.lang.ClassCastException (parse-conf "asdf")))))

(deftest build-clients
  (testing "client-builder builds clients"
    (let [conf (parse-conf (slurp (io/resource "example_conf.yml")))]
      (is (instance? cognitect.aws.client.Client (client-builder conf "s3"))))))