(defproject onyx-seq-sql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 [org.onyxplatform/onyx "0.11.0"]
                 [org.clojure/clojure "1.8.0"]
                 ;; derby is easy for a sql example as there is little setup
                 [org.clojure/java.jdbc "0.7.3"]
                 [org.apache.derby/derby "10.13.1.1"]
                 ;; for work:
                 ;;[com.microsoft.sqlserver/mssql-jdbc "6.1.0.jre8"]
                 ;;[org.postgresql/postgresql "42.1.4"]
                 ]
  :main ^:skip-aot onyx-seq-sql.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins []
  :resource-paths [
                   ;; other non-maven drivers?
                   ;;"Drivers/nzjdbc.jar"
                   ])
