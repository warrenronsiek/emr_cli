(ns emr-cli.utils.conf-parse
  (:require [clj-yaml.core :as yaml]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :refer [info spy]]))

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

(defmacro ^:private and-spec [defs]
  `(do ~@(map (fn [[name rest]] `(s/def ~name (s/and ~@rest))) defs)))

(defn validate-conf
  [conf]
  (and-spec [[::clusterName [string?]]
             [::logUri [string?]]
             [::subnet [string?]]
             [::pemKey [string?]]
             [::instanceCount [integer?]]
             [::bidPct [integer? #(and (< % 100) (< 0 %))]]
             [::instanceProfile [string?]]
             [::serviceRole [string?]]
             [::callerRole [string?]]
             [::jar [string?]]
             [::additionalSecurityGroups [string?]]
             [::shufflePartitions [integer?]]
             [::emrVersion [string?]]
             [::classification [string?]]
             [::key [string?]]
             [::value [string?]]
             [::jarClass [string?]]
             [::volumeCount [integer?]]
             [::volumeGB [integer?]]
             [::volumeType [string?]]])
  (s/def ::instanceType (set (map name (keys ec2-info))))
  (s/def ::properties (s/coll-of (s/keys :req-un [::key ::value])))
  (s/def ::configurations (s/coll-of (s/keys :req-un [::classification ::properties])))
  (s/def ::volumeConfiguration (s/coll-of (s/keys :req-un [::volumeCount ::volumeGB :volumeType])))
  (s/def ::jarArg (s/or :s string? :i int? :d double?))
  (s/def ::jarArgs (s/coll-of ::jarArg))
  (s/def ::tag (s/keys :Name :Value))
  (s/def ::tags (s/coll-of ::tag))
  (s/def ::config (s/keys :req-un [::clusterName ::logUri ::instanceType ::pemKey ::instanceCount
                                   ::instanceProfile ::serviceRole]
                          :opt-un [::tags ::callerRole ::jar ::jarClass ::jarArgs ::bidPct ::emrVersion
                                   ::configurations ::shufflePartitions ::additionalSecurityGroups
                                   ::volumeConfiguration]))
  (if (not (s/valid? ::config conf)) (s/explain ::config conf))
  (if (:jarClass conf) (assert (:jar conf)))
  (if (:jarArgs conf) (assert (:jar conf)))
  (s/conform ::config conf))


(defn parse-conf [conf] (let [vc (validate-conf (yaml/parse-string conf))]
                          (if (= vc :clojure.spec.alpha/invalid)
                            (throw (Exception. "invalid configuration file"))
                            vc)))

