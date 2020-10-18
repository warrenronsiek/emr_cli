(ns emr-cli.integration-test
  (:require [clojure.test :refer :all]
            [emr-cli.utils :refer [parse-conf client-builder]]))

(def ^:dynamic *cluster-id*)
(def emr-client (client-builder (parse-conf ("example_conf.yml")) "emr"))

(def cluster-fixture [test-fn]
  ())

(deftest creates-cluster
  "create cluster works"
  (testing "cluster creation"))