(ns emr-cli.state-test
  (:require [clojure.test :refer :all]
            [emr-cli.state :refer :all]))

(deftest cluster-ops
  (let [cluster-name (str (gensym "name"))
        cluster-id (str (gensym "id"))]
    (do
      (add-cluster {:cluster-name cluster-name :cluster-id cluster-id :pem-key "mykey" :region "region1"})
      (is (= ((keyword cluster-id) @emr-state) {:name cluster-name :pem "mykey" :region "region1"}))
      (is (= (first (get-cluster-ids cluster-name)) cluster-id))
      (remove-cluster cluster-id)
      (is (nil? ((keyword cluster-id) @emr-state))))))
