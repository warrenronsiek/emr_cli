(ns emr-cli.utils
  (:require [clj-yaml.core :as yaml]
            [bouncer.core :as b]
            [bouncer.validators :as v]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]))

(defn client-builder
  [config service & [region-override]]
  (let [service (keyword service)
        region (keyword (or region-override (:region config)))]
    (if (:callerRole config)
      (let [credentials (aws/invoke
                          (aws/client {:api :sts :region region})
                          {:op :AssumeRole :request {:RoleArn         (:callerRole config)
                                                     :RoleSessionName "emr-cli-session"}})]
        (aws/client {:api                  service
                     :region               region
                     :credentials-provider (credentials/profile-credentials-provider
                                             {:aws/access-key-id     (:Credentials (:AccessKeyId credentials))
                                              :aws/secret-access-key (:Credentials (:SecretAccessKey credentials))
                                              :aws/session-token     (:Credentials (:SessionToken credentials))})}))
      (aws/client {:api service :region region}))))

(defn parse-conf [conf]
  (let [validation (b/validate (yaml/parse-string conf)
                               :name [v/required v/string]
                               :logUri [v/required v/string]
                               :subnet [v/required v/string]
                               :instanceType [v/required v/string]
                               :key [v/required v/string]
                               :instanceCount [v/required v/integer]
                               :bidPct [v/integer]
                               :serviceRole [v/required v/string]
                               :instanceRole [v/required v/string]
                               :region [v/required v/string])]
    (if (first validation)
      (do (map #(println %1) (first validation))
          (throw (Exception. "config validation failed")))
      (second validation))))


(def ec2-info
  {:m4.4xlarge    {:memory 64.0 :cores 16}
   :m4.xlarge     {:memory 16.0 :cores 4}
   :m4.2xlarge    {:memory 32.0 :cores 8}
   :m4.10xlarge   {:memory 160.0 :cores 40}
   :m4.16xlarge   {:memory 256.0 :cores 64}
   :m5.4xlarge    {:memory 64.0 :cores 16}
   :m5.xlarge     {:memory 16.0 :cores 4}
   :m5.8xlarge    {:memory 128.0 :cores 32}
   :m5.2xlarge    {:memory 32.0 :cores 8}
   :m5.24xlarge   {:memory 384.0 :cores 96}
   :m5.16xlarge   {:memory 256.0 :cores 64}
   :m5.12xlarge   {:memory 192.0 :cores 48}
   :m5a.4xlarge   {:memory 64.0 :cores 16}
   :m5a.xlarge    {:memory 16.0 :cores 4}
   :m5a.2xlarge   {:memory 32.0 :cores 8}
   :m5a.24xlarge  {:memory 384.0 :cores 96}
   :m5a.12xlarge  {:memory 192.0 :cores 48}
   :m5d.4xlarge   {:memory 64.0 :cores 16}
   :m5d.xlarge    {:memory 16.0 :cores 4}
   :m5d.2xlarge   {:memory 32.0 :cores 8}
   :m5d.24xlarge  {:memory 384.0 :cores 96}
   :m5d.12xlarge  {:memory 192.0 :cores 48}
   :c4.4xlarge    {:memory 30.0 :cores 16}
   :c4.8xlarge    {:memory 60.0 :cores 36}
   :c4.2xlarge    {:memory 15.0 :cores 8}
   :c5.4xlarge    {:memory 32.0 :cores 16}
   :c5.2xlarge    {:memory 16.0 :cores 8}
   :c5.9xlarge    {:memory 72.0 :cores 36}
   :c5.18xlarge   {:memory 144.0 :cores 72}
   :c5d.4xlarge   {:memory 32.0 :cores 16}
   :c5d.2xlarge   {:memory 16.0 :cores 8}
   :c5d.9xlarge   {:memory 72.0 :cores 36}
   :c5d.18xlarge  {:memory 144.0 :cores 72}
   :c5n.4xlarge   {:memory 42.0 :cores 16}
   :c5n.2xlarge   {:memory 21.0 :cores 8}
   :c5n.9xlarge   {:memory 96.0 :cores 36}
   :c5n.18xlarge  {:memory 192.0 :cores 72}
   :z1d.xlarge    {:memory 32.0 :cores 4}
   :z1d.2xlarge   {:memory 64.0 :cores 8}
   :z1d.6xlarge   {:memory 192.0 :cores 24}
   :z1d.3xlarge   {:memory 96.0 :cores 12}
   :z1d.12xlarge  {:memory 384.0 :cores 48}
   :r3.4xlarge    {:memory 122.0 :cores 16}
   :r3.xlarge     {:memory 30.5 :cores 4}
   :r3.8xlarge    {:memory 244.0 :cores 32}
   :r3.2xlarge    {:memory 61.0 :cores 8}
   :r4.4xlarge    {:memory 122.0 :cores 16}
   :r4.xlarge     {:memory 30.5 :cores 4}
   :r4.8xlarge    {:memory 244.0 :cores 32}
   :r4.2xlarge    {:memory 61.0 :cores 8}
   :r4.16xlarge   {:memory 488.0 :cores 64}
   :r5.4xlarge    {:memory 128.0 :cores 16}
   :r5.xlarge     {:memory 32.0 :cores 4}
   :r5.2xlarge    {:memory 64.0 :cores 8}
   :r5.12xlarge   {:memory 384.0 :cores 48}
   :r5a.4xlarge   {:memory 128.0 :cores 16}
   :r5a.xlarge    {:memory 32.0 :cores 4}
   :r5a.2xlarge   {:memory 64.0 :cores 8}
   :r5a.24xlarge  {:memory 768.0 :cores 96}
   :r5a.12xlarge  {:memory 384.0 :cores 48}
   :r5d.4xlarge   {:memory 128.0 :cores 16}
   :r5d.xlarge    {:memory 32.0 :cores 4}
   :r5d.2xlarge   {:memory 64.0 :cores 8}
   :r5d.24xlarge  {:memory 768.0 :cores 96}
   :r5d.12xlarge  {:memory 384.0 :cores 48}
   :h1.4xlarge    {:memory 64.0 :cores 16}
   :h1.8xlarge    {:memory 128.0 :cores 32}
   :h1.16xlarge   {:memory 32.0 :cores 8}
   :i3.4xlarge    {:memory 122.0 :cores 16}
   :i3.xlarge     {:memory 30.5 :cores 4}
   :i3.8xlarge    {:memory 244.0 :cores 32}
   :i3.2xlarge    {:memory 61.0 :cores 8}
   :i3.16xlarge   {:memory 488.0 :cores 64}
   :i3en.xlarge   {:memory 32.0 :cores 4}
   :i3en.2xlarge  {:memory 64.0 :cores 8}
   :i3en.6xlarge  {:memory 192.0 :cores 24}
   :i3en.3xlarge  {:memory 96.0 :cores 12}
   :i3en.24xlarge {:memory 768.0 :cores 96}
   :i3en.12xlarge {:memory 384.0 :cores 48}
   :d2.4xlarge    {:memory 122.0 :cores 16}
   :d2.xlarge     {:memory 30.5 :cores 4}
   :d2.8xlarge    {:memory 244.0 :cores 36}
   :d2.2xlarge    {:memory 61.0 :cores 8}})