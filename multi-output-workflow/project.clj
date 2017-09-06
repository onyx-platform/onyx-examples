(defproject multi-output-workflow "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.10.1-20170906_012724-g64af78c"]]
  :plugins [[lein-update-dependency "0.1.2"]]
  :main ^:skip-aot multi-output-workflow.core)
