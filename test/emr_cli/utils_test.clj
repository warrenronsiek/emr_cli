(ns emr-cli.utils-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [emr-cli.utils.conf-parse :refer [parse-conf]]
            [emr-cli.utils :refer [client-builder get-emr-logs]]))


(deftest good-conf-consumption
  (testing "configs validate"
    (let [good-conf (parse-conf (slurp (io/resource "example_conf.yml")))]
      (is (= (:clusterName good-conf) "mycluster"))
      (is (= (:logUri good-conf) "s3://mys3bucket/logs/"))
      (is (= (:subnet good-conf) "subnet-0b087578ef9f3da24"))
      (is (= (:instanceType good-conf) "c5.4xlarge"))
      (is (= (:pemKey good-conf) "warren-laptop"))
      (is (= (:instanceCount good-conf) 2))
      (is (= (:bidPct good-conf) 50))
      (is (= (:serviceRole good-conf) "emr-default-role"))
      (is (= (:instanceProfile good-conf) "emr-default-instance-role")))))

(deftest bad-conf-consumption
  (testing "bad conf validation"
    (let [bad-conf (slurp (io/resource "bad_conf.yml"))]
      (is (= (parse-conf bad-conf) :clojure.spec.alpha/invalid)))))

(deftest crazy-conf-consumption
  (testing "not even wrong conf"
    (is (= :clojure.spec.alpha/invalid (parse-conf "asdf")))))

(deftest build-clients
  (testing "client-builder builds clients"
    (let [conf (parse-conf (slurp (io/resource "example_conf.yml")))]
      (is (instance? cognitect.aws.client.Client (client-builder conf "s3"))))))

;(deftest gets-logs
;  (testing "gets driver logs"
;    (let [logs (get-emr-logs "j-1T07SM8GGANJM" (parse-conf (slurp "lift.yml")))]
;      (is (nil? logs)))))