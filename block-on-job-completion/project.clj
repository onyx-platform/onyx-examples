(defproject block-on-job-completion "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.11.1-20171006_213950-g01c3e35"]]
  :plugins [[lein-update-dependency "0.1.2"]]
  :main ^:skip-aot block-on-job-completion.core)
