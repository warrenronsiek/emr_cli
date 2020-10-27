(defproject emr_cli "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cli-matic "0.4.3"]
                 [me.raynes/fs "1.4.6"]
                 [clj-commons/clj-yaml "0.7.1"]
                 [com.cognitect.aws/api "0.8.474"]
                 [com.cognitect.aws/endpoints "1.1.11.842"]
                 [com.cognitect.aws/s3 "809.2.734.0"]
                 [com.cognitect.aws/elasticmapreduce "801.2.704.0"]
                 [com.cognitect.aws/sts "798.2.678.0"]
                 [com.cognitect.aws/pricing "770.2.568.0"]]
  :repl-options {:init-ns emr-cli.core}
  :uberjar-name "emr_cli.jar"
  :main emr_cli.core
  :profiles {:uberjar {:aot :all}})
