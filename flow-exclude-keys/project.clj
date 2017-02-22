(defproject flow-exclude-keys "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.10.0-20170222_194729-ga0c259d"]]
  :plugins [[lein-update-dependency "0.1.2"]])
