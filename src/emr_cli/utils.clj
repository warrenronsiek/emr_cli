(ns emr-cli.utils
  (:require [clj-yaml.core :as yaml]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [cognitect.aws.credentials :as credentials]
            [clojure.string :as str]
            [taoensso.timbre :refer [info]]
            [clojure.java.io :as io])
  (:import (java.io IOException)
           (java.time Duration ZoneId ZonedDateTime Instant)))

(defmacro ^:private and-spec [defs]
  `(do ~@(map (fn [[name rest]] `(s/def ~name (s/and ~@rest))) defs)))

(defn validate-conf
  [conf]
  (and-spec [[::clusterName [string?]]
             [::logUri [string?]]
             [::subnet [string?]]
             [::instanceType [string?]]
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
             [::jarClass [string?]]])
  (s/def ::properties (s/coll-of (s/keys :req-un [::key ::value])))
  (s/def ::configurations (s/coll-of (s/keys :req-un [::classification ::properties])))
  (s/def ::jarArg (s/or :s string? :i int? :d double?))
  (s/def ::jarArgs (s/coll-of ::jarArg))
  (s/def ::tag (s/keys :Name :Value))
  (s/def ::tags (s/coll-of ::tag))
  (s/def ::config (s/keys :req-un [::clusterName ::logUri ::instanceType ::pemKey ::instanceCount
                                   ::instanceProfile ::serviceRole]
                          :opt-un [::tags ::callerRole ::jar ::jarClass ::jarArgs ::bidPct ::emrVersion
                                   ::configurations ::shufflePartitions ::additionalSecurityGroups]))
  (if (not (s/valid? ::config conf)) (s/explain ::config conf))
  (if (:jarClass conf) (assert (:jar conf)))
  (if (:jarArgs conf) (assert (:jar conf)))
  (s/conform ::config conf))


(defn parse-conf [conf] (validate-conf (yaml/parse-string conf)))

(defn ^:private session-credentials-provider
  "need to have this weird glue thing as a consequence of the way providers are implemented"
  [{:keys [access-key-id secret-access-key session-token]}]
  (assert access-key-id "Missing")
  (assert secret-access-key "Missing")
  (assert session-token "Missing")
  (reify credentials/CredentialsProvider
    (fetch [_]
      {:aws/access-key-id     access-key-id
       :aws/secret-access-key secret-access-key
       :aws/session-token     session-token})))

(defn client-builder
  [config service & [region-override]]
  (let [service (keyword (if (= service "emr") "elasticmapreduce" service))
        region (keyword (or region-override (:region config) :us-east-1))]
    (if (:callerRole config)
      (let [credentials (aws/invoke
                          (aws/client {:api :sts :region region})
                          {:op :AssumeRole :request {:RoleArn         (:callerRole config)
                                                     :RoleSessionName "emr-cli-session"}})
            keys (:Credentials credentials)]
        (aws/client {:api                  service
                     :region               region
                     :credentials-provider (session-credentials-provider
                                             {:access-key-id     (:AccessKeyId keys)
                                              :secret-access-key (:SecretAccessKey keys)
                                              :session-token     (:SessionToken keys)})}))
      (aws/client {:api service :region region}))))

(defn get-region [config]
  (let [client (client-builder config "ec2")
        subnet (aws/invoke client {:op :DescribeSubnets :request {:SubnetIds [(:subnet config)]}})
        sub-region (-> subnet :Subnets first :AvailabilityZone)]
    (if (some? (-> subnet :Response :Errors :Error :Code))
      (throw (Exception. (str (-> subnet :Response :Errors :Error :Message))))
      (str/join (drop-last 1 sub-region)))))

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

(defn log-filter [lines]
  (filter #(and (not (str/includes? % "BlockManagerInfo"))
                (not (str/includes? % "BlockManagerMasterEndpoint"))
                (not (str/includes? % "SharedState"))
                (not (str/includes? % "ServerInfo"))
                (not (str/includes? % "ContextHandler"))
                (not (str/includes? % "ExecutorMonitor"))
                (not (str/includes? % "YarnSchedulerBackend"))
                (not (str/includes? % "TaskSetManager"))
                (not (str/includes? % "YarnClusterScheduler"))
                (not (str/includes? % "DAGScheduler"))
                (not (str/includes? % "ApplicationMaster"))
                (not (str/includes? % "EmrOptimizedParquetOutputCommitter"))
                (not (str/includes? % "MemoryStore"))
                (not (str/includes? % "MapOutputTrackerMasterEndpoint"))
                (not (str/includes? % "TypeUtil"))
                (not (str/includes? % "SQLHadoopMapReduceCommitProtocol"))
                (not (str/includes? % "MapOutputTrackerMasterEndpoint"))
                (not (str/includes? % "MultipartUploadOutputStream"))
                (not (str/includes? % "SQLHadoopMapReduceCommitProtocol"))
                (not (str/includes? % "YarnAllocator"))
                (not (str/includes? % "ExecutorAllocationManager"))
                (not (str/includes? % "CodeGenerator"))
                (not (str/includes? % "YarnAllocator"))
                (not (str/includes? % "FileSourceStrategy"))
                (not (str/includes? % "ClientConfigurationFactory"))
                (not (str/includes? % "SparkContext"))
                (not (str/includes? % "YarnAllocator"))
                (not (str/includes? % "CacheManager"))
                (not (str/includes? % "InMemoryFileIndex"))
                (not (str/includes? % "FileSourceScanExec"))
                (not (str/includes? % "ParquetFileFormat"))
                (not (str/includes? % "ContextCleaner"))
                (not (str/includes? % "Manager"))
                (not (str/includes? % "ApplicationMaster"))
                (not (str/includes? % "AdaptiveSparkPlanExec"))
                (not (str/includes? % "MapOutputTracker"))
                (not (str/includes? % "HashAggregateExec"))
                (not (str/includes? % "FileOutputCommitter"))
                (not (str/includes? % "DeltaLogFileIndex"))
                (not (str/includes? % "Snapshot")))
          lines))

(defn get-emr-logs [cluster-id conf]
  (let [s3-client (client-builder conf "s3")
        uri-components (str/split (str/join "/" [(str/join (drop 5 (:logUri conf))) cluster-id]) #"/")
        bucket (first uri-components)
        prefix (str/join "/" (filter #(not (= "" %)) (rest uri-components)))
        container-logs (aws/invoke s3-client {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}})
        stderr-files (filter #(str/ends-with? % "stderr.gz") (map :Key (:Contents container-logs)))
        driver-logs (filter #(str/includes? % "000001") stderr-files)
        _ (try (io/delete-file "/tmp/stderr")
               (catch IOException _ "file probably doesnt exist"))
        get (aws/invoke s3-client {:op :GetObject :request {:Bucket bucket
                                                            :Key    (first driver-logs)}})
        _ (io/copy (:Body get) (io/file "/tmp/stderr"))
        logs (log-filter (str/split (slurp "/tmp/stderr") #"\n"))]
    (doseq [line logs] (println line))))

(defn get-cluster-status [cluster-id conf]
  (let [emr-client (client-builder conf "emr")
        status(:Status (:Cluster (aws/invoke emr-client {:op :DescribeCluster :request {:ClusterId cluster-id}})))
        state (:State status)
        {start :CreationDateTime end :EndDateTime} (:Timeline status)]
    (do
      (println "State: " state)
      (println "Currently: " (:Message (:StateChangeReason status)))
      (println "Runtime: " (let [mins (case state
                                        "RUNNING" (.toMinutes (Duration/between (.toInstant start) (Instant/now)))
                                        "TERMINATED" (.toMinutes (Duration/between (.toInstant start) (.toInstant end)))
                                        "TERMINATING" (.toMinutes (Duration/between (.toInstant start) (.toInstant end)))
                                        (.toMinutes (Duration/between (.toInstant start) (Instant/now))))
                                 hrs (int (Math/floor (/ mins 60.0)))
                                 remaining-mins (mod mins 60)]
                             (str hrs "hr " remaining-mins "min"))))))
