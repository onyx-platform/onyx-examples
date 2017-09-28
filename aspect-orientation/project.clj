(defproject aspect-orientation "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.11.1-20170928_061857-ge055991"]
                 [dire "0.5.2"]]
  :plugins [[lein-update-dependency "0.1.2"]]
  :main ^:skip-aot aspect-orientation.core)
