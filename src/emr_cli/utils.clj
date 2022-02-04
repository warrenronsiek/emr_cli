(ns emr-cli.utils
  (:require [clj-yaml.core :as yaml]
            [cognitect.aws.client.api :as aws]
            [clojure.spec.alpha :as s]
            [cognitect.aws.credentials :as credentials]
            [clojure.string :as str]
            [taoensso.timbre :refer [info spy error]]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.test :as t])
  (:import (java.io IOException BufferedInputStream, BufferedReader, File)
           (java.lang IllegalArgumentException)
           (java.time Duration ZoneId ZonedDateTime Instant)
           (java.util.zip GZIPInputStream)))

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
                (not (str/includes? % "ExecutorAllocationManager"))
                (not (str/includes? % "CodeGenerator"))
                (not (str/includes? % "YarnAllocator"))
                (not (str/includes? % "FileSourceStrategy"))
                (not (str/includes? % "ClientConfigurationFactory"))
                (not (str/includes? % "SparkContext"))
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
                (not (str/includes? % "DataSourceStrategy"))
                (not (str/includes? % "MergeIntoCommand"))
                (not (str/includes? % "FileFormatWriter"))
                (not (str/includes? % "MapPartitionsRDD"))
                (not (str/includes? % "DeltaLogFileIndex"))
                (not (str/includes? % "TorrentBroadcast"))
                (not (str/includes? % "DelegatingLogStore"))
                (not (str/includes? % "DefaultCachedBatchSerializer"))
                (not (str/includes? % "YarnCoarseGrainedExecutorBackend"))
                (not (str/includes? % "ShuffleBlockFetcherIterator"))
                (not (str/includes? % "DeltaLog"))
                (not (str/includes? % "Snapshot"))
                (not (str/includes? % "AsyncEventQueue"))
                (not (str/includes? % "VacuumCommand"))
                (not (str/includes? % "TransportClientFactory"))
                (not (str/includes? % "OptimisticTransaction"))
                (not (str/includes? % "Executor")))
          lines))

(defn get-emr-logs [cluster-id conf]
  (let [s3-client (client-builder conf "s3")
        uri-components (str/split (str/join "/" [(str/join (drop 5 (:logUri conf))) cluster-id]) #"/")
        bucket (first uri-components)
        prefix (str/join "/" (filter #(not (= "" %)) (rest uri-components)))
        driver-logs (loop [s3-resp (aws/invoke s3-client {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}})]
                      (let [driver-logs (first (filter #(some? (re-matches #".*application.*0002.*stderr\.gz$" %))
                                                       (map :Key (:Contents s3-resp))))]
                        (cond
                          (string? driver-logs) (do
                                                  (info (str "found log file: " driver-logs))
                                                  driver-logs)
                          (:IsTruncated s3-resp) (do
                                                   (info (str "lots of logs, iterating through them..."))
                                                   (recur (aws/invoke s3-client {:op      :ListObjectsV2
                                                                                 :request {:Bucket            bucket
                                                                                           :Prefix            prefix
                                                                                           :ContinuationToken (:ContinuationToken s3-resp)}})))
                          :else (throw (Exception. "driver logs dont exist, this is either due to a spark driver crash or an incorrect logUri configuration param.")))))
        get (aws/invoke s3-client {:op :GetObject :request {:Bucket bucket
                                                            :Key    driver-logs}})]
    (try (doseq [line (log-filter (line-seq (io/reader (:Body get))))] (println line))
         (catch IllegalArgumentException e (error "failed to s3 get the file. Its probably large and your internet is slow.")))))

(defn get-cluster-status [cluster-id conf]
  (let [emr-client (client-builder conf "emr")
        status (:Status (:Cluster (aws/invoke emr-client {:op :DescribeCluster :request {:ClusterId cluster-id}})))
        state (:State status)
        {start :CreationDateTime end :EndDateTime} (:Timeline status)]
    (do
      (println "State: " state)
      (println "Currently: " (:Message (:StateChangeReason status)))
      (println "Runtime: " (let [mins (case state
                                        "RUNNING" (.toMinutes (Duration/between (.toInstant start) (Instant/now)))
                                        "TERMINATED" (.toMinutes (Duration/between (.toInstant start) (.toInstant end)))
                                        "TERMINATING" (.toMinutes (Duration/between (.toInstant start) (Instant/now)))
                                        (.toMinutes (Duration/between (.toInstant start) (Instant/now))))
                                 hrs (int (Math/floor (/ mins 60.0)))
                                 remaining-mins (mod mins 60)]
                             (str hrs "hr " remaining-mins "min"))))))
