(ns emr-cli.state-test
  (:require [clojure.test :refer :all]
            [emr-cli.state :refer :all]))

(deftest cluster-ops
  (let [cluster-name (gensym "name")
        cluster-id (gensym "id")]
    (do
      (add-cluster {:cluster-name cluster-name :cluster-id cluster-id :pem-key "mykey"})
      (is (= ((keyword cluster-id) @emr-state) {:name cluster-name :pem "mykey"}))
      (remove-cluster cluster-id)
      (is (nil? ((keyword cluster-id) @emr-state))))))
